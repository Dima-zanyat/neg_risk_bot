"""
Сканер арбитражных возможностей в NegRisk-событиях.

Читает то, что уже собрал Collector (рынки + недавние снепшоты ордербуков),
и ищет два вида возможностей в многоисходных NegRisk-событиях по формуле
контракта NegRiskAdapter (проверена построчно по верифицированному
исходнику на PolygonScan):

  convert_all    - купить No по ВСЕМ исходам события, сконвертировать все
                    сразу одним вызовом convertPositions с indexSet на все
                    исходы. Контракт в этом случае отдаёт (n-1) * amount
                    прямо в pUSD и НЕ выдаёт Yes-токены вообще (n - k = 0,
                    когда k = n) - то есть после этого шага не остаётся
                    вообще никакой позиции, которую нужно ещё продавать.
                    Это и есть по-настоящему безрисковый (после покупки
                    No) путь к pUSD - именно то, что в описании стратегии
                    называлось «Merge» на уровне идеи, хотя технически
                    отдельного вызова merge через все исходы в контракте
                    НЕТ: pUSD возвращает сам convertPositions.

  convert_single - купить No по ОДНОМУ исходу; Convert превращает его в
                    Yes по каждому из остальных исходов (pUSD в этом
                    случае не возвращается вовсе, т.к. multiplier = k-1 = 0
                    при k=1). Прибыль реализуется, только если полученный
                    пакет Yes-токенов удастся продать по текущим bid -
                    то есть после Convert остаётся позиция, несущая
                    рыночный риск, пока её не закрыли.

Модуль только читает и считает - ничего не покупает и не отправляет
транзакций. Найденные возможности пишутся в `negrisk_opportunities` для
просмотра / для алертов в Telegram-боте.

Известные ограничения (важно прочитать, прежде чем доверять цифрам):
  - Augmented NegRisk-события (с ещё не раскрытыми placeholder-исходами,
    например «Other» до называния всех кандидатов) пропускаются целиком -
    документация Polymarket прямо предупреждает, что смысл «Other»
    меняется по ходу события, торговать им напрямую не стоит.
  - Комиссия конвертации учтена по формуле из исходника контракта:
    feeAmount = amount * feeBips / FEE_DENOMINATOR,
    amountOut = amount - feeAmount - именно amountOut реально приходит
    трейдеру (в виде pUSD для convert_all, в виде количества Yes-токенов
    для convert_single). Это уже не приближение, а формула из кода.
  - Глубина книги учитывается (см. book_math.py), но цены могут
    измениться между моментом сканирования и моментом реального
    выставления ордера - это генератор сигналов, а не гарантия
    исполнения. Все риски, перечисленные в описании стратегии
    (проскальзывание, частичное исполнение, движение цены, ошибки
    резолюции) по-прежнему в силе поверх того, что здесь посчитано.
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

from app.config import get_settings
from app.repository.database import async_session_factory
from app.repository.market_repo import (
    get_active_negrisk_markets,
    get_latest_snapshots_for_tokens,
    save_opportunity,
)
from app.repository.models import Market
from app.services.polymarket.negrisk_adapter import NegRiskAdapterClient
from app.services.strategy.book_math import walk_book

logger = logging.getLogger(__name__)


class NegRiskScanner:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._stop_event = asyncio.Event()
        self._fee_bips_cache: dict[str, int] = {}
        self._adapter: NegRiskAdapterClient | None = None  # один общий клиент, создаётся лениво
        self._fee_lookups_this_cycle = 0
        self.last_cycle_at: datetime | None = None
        self.last_cycle_error: str | None = None
        self.last_opportunity_count: int = 0

    def stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self) -> None:
        logger.info(
            "Сканер NegRisk запущен (interval=%sс, size=%s, min_profit=%.2f%%)",
            self._settings.NEGRISK_SCAN_INTERVAL_SECONDS,
            self._settings.NEGRISK_SCAN_SIZE,
            self._settings.NEGRISK_MIN_PROFIT_PCT * 100,
        )
        while not self._stop_event.is_set():
            try:
                self.last_opportunity_count = await self.run_once()
                self.last_cycle_error = None
            except Exception as exc:  # noqa: BLE001 - сканер не должен умирать
                self.last_cycle_error = str(exc)
                logger.exception("Цикл сканера упал, повторим на следующей итерации")
            self.last_cycle_at = datetime.now(timezone.utc)

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._settings.NEGRISK_SCAN_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

    async def run_once(self) -> int:
        async with async_session_factory() as session:
            markets = await get_active_negrisk_markets(session)
            events = _group_by_event(markets)

            all_token_ids = [
                token_id
                for outcomes in events.values()
                for m in outcomes
                for token_id in (m.clob_token_ids or [])
            ]
            books = await get_latest_snapshots_for_tokens(session, all_token_ids)

            opportunities = []
            self._fee_lookups_this_cycle = 0
            for market_id, outcomes in events.items():
                # Комиссия контракта может только УМЕНЬШИТЬ прибыль, поэтому
                # сначала считаем без неё (оптимистичная оценка): если и так
                # ничего нет - событие отсеяно, и в цепочку ходить не нужно.
                # Раньше комиссия запрашивалась для КАЖДОГО события синхронным
                # web3-вызовом прямо в event loop - сотни блокирующих HTTPS-
                # запросов к публичному RPC подряд замораживали весь процесс
                # (включая FastAPI) на часы.
                if not self._scan_event(market_id, outcomes, books, fee_bips=0):
                    continue
                fee_bips = await self._fee_bips(market_id)
                if fee_bips is None:
                    continue  # лимит запросов за цикл исчерпан - вернёмся в следующем
                opportunities.extend(self._scan_event(market_id, outcomes, books, fee_bips))

            for opp in opportunities:
                await save_opportunity(session, opp)
            await session.commit()

        for opp in opportunities:
            logger.info(
                "Возможность: %s market=%s profit=%.4f (%.2f%%) size=%s",
                opp["kind"],
                opp["neg_risk_market_id"],
                opp["est_profit"],
                opp["est_profit_pct"] * 100,
                opp["size"],
            )
        return len(opportunities)

    async def _fee_bips(self, market_id: str) -> int | None:
        """Комиссия конвертации для рынка. Чтение с цепочки идёт в потоке
        (web3 синхронный - в event loop его вызывать нельзя) и под общим
        таймаутом. Результат, в том числе запасное значение при сбое,
        кэшируется: повторно в цепочку за тем же рынком не ходим.
        None - превышен лимит запросов за цикл (NEGRISK_MAX_FEE_LOOKUPS_PER_CYCLE)."""
        if market_id in self._fee_bips_cache:
            return self._fee_bips_cache[market_id]
        if self._fee_lookups_this_cycle >= self._settings.NEGRISK_MAX_FEE_LOOKUPS_PER_CYCLE:
            logger.debug("Лимит запросов комиссии за цикл исчерпан, %s отложен", market_id)
            return None
        self._fee_lookups_this_cycle += 1

        if self._adapter is None:
            self._adapter = NegRiskAdapterClient()
        timeout = self._settings.POLYGON_RPC_TIMEOUT_SECONDS * 2
        try:
            fee = await asyncio.wait_for(
                asyncio.to_thread(self._adapter.get_fee_bips, market_id), timeout=timeout
            )
        except Exception:  # noqa: BLE001 - сбой/таймаут RPC: запасное значение
            logger.warning(
                "Не удалось прочитать комиссию on-chain для %s, беру запасное значение %s б.п.",
                market_id, self._settings.NEGRISK_FEE_BIPS_FALLBACK,
            )
            fee = self._settings.NEGRISK_FEE_BIPS_FALLBACK
        self._fee_bips_cache[market_id] = fee
        return fee

    def _scan_event(
        self,
        market_id: str,
        outcomes: list[Market],
        books: dict,
        fee_bips: int,
    ) -> list[dict]:
        settings = self._settings
        size = settings.NEGRISK_SCAN_SIZE
        fee_rate = fee_bips / 10_000 if fee_bips else 0.0
        # Именно столько токенов/pUSD реально возвращает convertPositions
        # с одной "единицы" сожжённых No - формула прямо из исходника
        # контракта (amountOut = _amount - feeAmount).
        amount_out = size * (1 - fee_rate)

        # Augmented-события (с нераскрытыми placeholder-исходами) пока не
        # трогаем - см. предупреждение в докстринге модуля.
        if any(m.neg_risk_augmented for m in outcomes):
            return []

        legs = []
        for m in outcomes:
            if not m.clob_token_ids or len(m.clob_token_ids) < 2:
                continue
            # Без индекса исхода нельзя безопасно построить _indexSet для
            # convertPositions - без него даже не пытаемся считать эту
            # возможность (лучше пропустить рынок, чем угадать индекс).
            if m.neg_risk_outcome_index is None:
                continue
            yes_token, no_token = m.clob_token_ids[0], m.clob_token_ids[1]
            if yes_token not in books or no_token not in books:
                continue
            legs.append(
                {
                    "condition_id": m.condition_id,
                    "question": m.question,
                    "yes_token": yes_token,
                    "no_token": no_token,
                    "outcome_index": m.neg_risk_outcome_index,
                }
            )
        if len(legs) < 2:
            return []

        found: list[dict] = []
        n = len(legs)

        # --- convert_all: покупаем No по КАЖДОМУ исходу и конвертируем всё
        # одним вызовом (indexSet = все исходы) -> чистый pUSD, без остатка
        # в виде Yes-токенов. После заполнения ордеров на покупку дальше
        # уже ничего не может сорваться на стороне продажи - продавать
        # тут просто нечего. ---
        total_cost = 0.0
        fully_filled = True
        per_leg_cost = {}
        for leg in legs:
            book = books[leg["no_token"]].raw_book
            cost, filled = walk_book(book, "buy", size)
            per_leg_cost[leg["condition_id"]] = cost
            total_cost += cost
            if filled < size:
                fully_filled = False

        if fully_filled and total_cost > 0:
            value = (n - 1) * amount_out
            profit = value - total_cost
            profit_pct = profit / total_cost if total_cost else 0.0
            if profit_pct >= settings.NEGRISK_MIN_PROFIT_PCT:
                found.append(
                    {
                        "neg_risk_market_id": market_id,
                        "kind": "convert_all",
                        "outcome_condition_id": None,
                        "size": size,
                        "est_cost": total_cost,
                        "est_value": value,
                        "est_fee": total_cost * fee_rate,  # справочно; уже учтено в value через amount_out
                        "est_profit": profit,
                        "est_profit_pct": profit_pct,
                        "details": {
                            "legs": [
                                {
                                    "condition_id": leg["condition_id"],
                                    "question": leg["question"],
                                    "no_token": leg["no_token"],
                                    "outcome_index": leg["outcome_index"],
                                }
                                for leg in legs
                            ],
                            "per_leg_cost": per_leg_cost,
                            "fee_bips": fee_bips,
                        },
                    }
                )

        # --- convert_single: покупаем No по ОДНОМУ исходу, Convert даёт
        # пакет Yes по остальным исходам (pUSD в этом случае не
        # возвращается - k=1 => multiplier=0). Оцениваем пакет по текущим
        # bid, как если бы мы сразу же его распродали. ---
        for i, leg in enumerate(legs):
            no_book = books[leg["no_token"]].raw_book
            cost, filled = walk_book(no_book, "buy", size)
            if filled < size or cost <= 0:
                continue

            value = 0.0
            other_fully_bid = True
            for j, other in enumerate(legs):
                if j == i:
                    continue
                yes_book = books[other["yes_token"]].raw_book
                proceeds, filled_sell = walk_book(yes_book, "sell", amount_out)
                value += proceeds
                if filled_sell < amount_out:
                    other_fully_bid = False

            if not other_fully_bid or value <= 0:
                continue

            profit = value - cost
            profit_pct = profit / cost if cost else 0.0
            if profit_pct >= settings.NEGRISK_MIN_PROFIT_PCT:
                found.append(
                    {
                        "neg_risk_market_id": market_id,
                        "kind": "convert_single",
                        "outcome_condition_id": leg["condition_id"],
                        "size": size,
                        "est_cost": cost,
                        "est_value": value,
                        "est_fee": size * fee_rate,
                        "est_profit": profit,
                        "est_profit_pct": profit_pct,
                        "details": {
                            "bought_no_on": leg["question"],
                            "no_token": leg["no_token"],
                            "outcome_index": leg["outcome_index"],
                            "converts_into_yes_on": [
                                {
                                    "question": o["question"],
                                    "yes_token": o["yes_token"],
                                    "outcome_index": o["outcome_index"],
                                }
                                for j, o in enumerate(legs)
                                if j != i
                            ],
                            "fee_bips": fee_bips,
                        },
                    }
                )

        return found


def _group_by_event(markets: list[Market]) -> dict[str, list[Market]]:
    grouped: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        if m.neg_risk_market_id:
            grouped[m.neg_risk_market_id].append(m)
    return grouped


async def _main() -> None:
    from app.core.logging import setup_logging
    from app.repository.database import init_db

    settings = get_settings()
    setup_logging(settings.LOG_LEVEL)
    await init_db()
    await NegRiskScanner().run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
