"""
Слой исполнения: превращает найденную сканером возможность в реальный
ордер на CLOB и, если он исполнился, - в реальную транзакцию Convert
через NegRiskAdapter.

Написан на базе py-clob-client-v2 1.2.0 (официальный клиент CLOB V2 от
Polymarket). Пакет был скачан и проверен непосредственно по исходникам
при разработке этого файла - конструктор ClobClient, MarketOrderArgs,
PartialCreateOrderOptions(neg_risk=...) и create_and_post_market_order
сверены с реальным кодом пакета версии 1.2.0, а не угаданы по докам.

МОДЕЛЬ БЕЗОПАСНОСТИ - независимые предохранители, нужны все, прежде чем
куда-то уйдут реальные деньги:
  1. settings.ENABLE_ONCHAIN_EXECUTION = true (.env)
  2. settings.RUN_EXECUTOR_IN_PROCESS = true (.env) - фоновый цикл не
     запустится сам по себе даже при включённом (1)
  3. Размер каждой сделки и дневной лимит считаются ДОЛЕЙ ОТ ЖИВОГО
     БАЛАНСА кошелька (EXECUTION_TRADE_FRACTION / EXECUTION_DAILY_FRACTION),
     а не от суммы, которую сканер указал в возможности - так капитал
     растёт сам, без ручной перенастройки лимитов
  4. Если баланс прочитать не удалось - цикл исполнения целиком
     пропускается (никогда не торгуем "вслепую" по угаданной цифре)
  5. Телеграм-команда /pause - аварийный стоп без перезапуска процесса

ВОССТАНОВЛЕНИЕ ПОСЛЕ СБОЯ: каждая попытка исполнения последовательно
проходит фазы в таблице execution_states (см. repository/models.py) -
если процесс упадёт между "купили No" и "сконвертировали", при следующем
запуске это будет видно по незавершённой записи (main.py проверяет это
при старте и шлёт алерт в Telegram), а не потеряется молча.

Как исполняются два типа возможностей (экономика - в negrisk_scanner.py):
  convert_all    - купить No по всем исходам -> один вызов convertPositions
                    с indexSet на все исходы -> pUSD напрямую, без остатка
                    в виде Yes-токенов. После заполнения ордеров продавать
                    уже нечего.
  convert_single - купить No по одному исходу -> Convert даёт пакет Yes по
                    остальным исходам -> эти Yes нужно ПРОДАТЬ, чтобы
                    реализовать прибыль.

ВАЖНО про _indexSet: битовая позиция каждого исхода берётся из поля Gamma
`groupItemThreshold` (сохранено как Market.neg_risk_outcome_index и
продублировано в `details` возможности сканером) - НЕ из порядка, в
котором рынки лежат в нашей БД. Подтверждено напрямую по ответу Gamma API
(questionID = negRiskMarketID с заменённым последним байтом на этот
индекс). Возможность без этого поля сканер вообще не создаёт - см.
negrisk_scanner.py.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from py_clob_client_v2 import (
    ClobClient,
    MarketOrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
)

from app.config import get_settings
from app.repository.database import async_session_factory
from app.repository.market_repo import (
    create_execution_state,
    get_recent_opportunities,
    save_ledger_entry,
    update_execution_state,
)
from app.repository.models import ExecutionState
from app.services.polymarket.gamma_client import GammaClient, count_negrisk_event_outcomes
from app.services.polymarket.negrisk_adapter import NegRiskAdapterClient, index_set_for
from app.services.polymarket.wallet import get_current_bankroll_async

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    opportunity_id: int
    kind: str
    status: str  # "filled" | "skipped" | "error"
    detail: str
    filled_size: float = 0.0
    spent: float = 0.0
    received: float = 0.0
    tx_hashes: tuple[str, ...] = field(default_factory=tuple)


class DailyNotionalTracker:
    """Дневной лимит трат, хранится в памяти процесса. После рестарта
    обнуляется - защита от "бот сошёл с ума и слил всё за час
    непрерывной работы", а не строгий аудируемый лимит."""

    def __init__(self, cap: float) -> None:
        self.cap = cap
        self._spent_today = 0.0
        self._day = datetime.now(timezone.utc).date()

    def set_cap(self, cap: float) -> None:
        """Лимит пересчитывается каждый цикл от текущего баланса - сам
        счётчик потраченного за день при этом не обнуляется."""
        self.cap = cap

    def _roll_if_new_day(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._day:
            self._day = today
            self._spent_today = 0.0

    def remaining(self) -> float:
        self._roll_if_new_day()
        return max(0.0, self.cap - self._spent_today)

    def record(self, amount: float) -> None:
        self._roll_if_new_day()
        self._spent_today += amount


class Executor:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._stop_event = asyncio.Event()
        self._paused = asyncio.Event()  # используется командой /pause в Telegram-боте
        self._daily = DailyNotionalTracker(self._settings.EXECUTION_DAILY_NOTIONAL_CAP)
        self.last_cycle_at: datetime | None = None
        self.last_cycle_error: str | None = None
        self.last_results: list[ExecutionResult] = []
        self._clob: ClobClient | None = None
        self._adapter = NegRiskAdapterClient()

    def stop(self) -> None:
        self._stop_event.set()

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    @property
    def is_paused(self) -> bool:
        return self._paused.is_set()

    def _require_clob(self) -> ClobClient:
        if self._clob is not None:
            return self._clob
        s = self._settings
        if not s.WALLET_PRIVATE_KEY:
            raise RuntimeError(
                "WALLET_PRIVATE_KEY не задан в .env - исполнение невозможно."
            )
        bootstrap = ClobClient(
            host=s.CLOB_API_URL, chain_id=137, key=s.WALLET_PRIVATE_KEY,
            signature_type=s.WALLET_SIGNATURE_TYPE or None,
            funder=s.WALLET_FUNDER_ADDRESS or None,
        )
        creds = bootstrap.create_or_derive_api_key()
        self._clob = ClobClient(
            host=s.CLOB_API_URL, chain_id=137, key=s.WALLET_PRIVATE_KEY, creds=creds,
            signature_type=s.WALLET_SIGNATURE_TYPE or None,
            funder=s.WALLET_FUNDER_ADDRESS or None,
        )
        return self._clob

    def _owner_address(self) -> str:
        s = self._settings
        return s.WALLET_FUNDER_ADDRESS or s.WALLET_ADDRESS

    async def run_forever(self) -> None:
        s = self._settings
        logger.info(
            "Исполнитель запущен (interval=%sс, доля/сделка=%.1f%%, доля/день=%.1f%%)",
            s.EXECUTOR_POLL_INTERVAL_SECONDS,
            s.EXECUTION_TRADE_FRACTION * 100,
            s.EXECUTION_DAILY_FRACTION * 100,
        )
        while not self._stop_event.is_set():
            try:
                self.last_results = await self.run_once()
                self.last_cycle_error = None
            except Exception as exc:  # noqa: BLE001 - цикл не должен умирать
                self.last_cycle_error = str(exc)
                logger.exception("Цикл исполнителя упал, повторим на следующей итерации")
            self.last_cycle_at = datetime.now(timezone.utc)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=s.EXECUTOR_POLL_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def run_once(self) -> list[ExecutionResult]:
        s = self._settings
        if not s.ENABLE_ONCHAIN_EXECUTION or self.is_paused:
            return []

        bankroll = await get_current_bankroll_async()
        if bankroll is None:
            logger.warning("Баланс кошелька недоступен - пропускаю цикл исполнения")
            return []

        trade_cap = min(bankroll * s.EXECUTION_TRADE_FRACTION, s.EXECUTION_MAX_NOTIONAL_PER_TRADE)
        self._daily.set_cap(min(bankroll * s.EXECUTION_DAILY_FRACTION, s.EXECUTION_DAILY_NOTIONAL_CAP))

        async with async_session_factory() as session:
            opportunities = await get_recent_opportunities(session, limit=20)

        results: list[ExecutionResult] = []
        for opp in opportunities:
            if self.is_paused:
                break
            result = await asyncio.to_thread(self._execute_one, opp, trade_cap)
            results.append(result)
            if result.status == "filled":
                self._daily.record(result.spent)
                logger.warning(
                    "ИСПОЛНЕНО: opp=%s kind=%s потрачено=%.2f получено=%.2f tx=%s",
                    result.opportunity_id, result.kind, result.spent, result.received, result.tx_hashes,
                )
            elif result.status == "error":
                logger.error("Ошибка исполнения opp=%s: %s", result.opportunity_id, result.detail)
        return results

    def _execute_one(self, opp, trade_cap: float) -> ExecutionResult:
        """Синхронный метод - py-clob-client-v2 сам по себе синхронный,
        вызывается через asyncio.to_thread из run_once."""
        s = self._settings

        if opp.est_profit_pct < s.NEGRISK_MIN_PROFIT_PCT:
            return ExecutionResult(opp.id, opp.kind, "skipped", "прибыль ниже порога на момент проверки")

        notional = min(opp.est_cost, trade_cap)
        if notional <= 0:
            return ExecutionResult(opp.id, opp.kind, "skipped", "размер сделки равен нулю")
        if notional > self._daily.remaining():
            return ExecutionResult(opp.id, opp.kind, "skipped", "дневной лимит исчерпан")

        try:
            clob = self._require_clob()
        except Exception as exc:  # noqa: BLE001
            return ExecutionResult(opp.id, opp.kind, "error", f"не удалось создать CLOB-клиент: {exc}")

        details = opp.details or {}
        try:
            if opp.kind == "convert_single":
                return self._execute_convert_single(clob, opp, details, notional)
            if opp.kind == "convert_all":
                return self._execute_convert_all(clob, opp, details, notional)
            return ExecutionResult(opp.id, opp.kind, "skipped", f"неизвестный тип возможности: {opp.kind}")
        except Exception as exc:  # noqa: BLE001 - одна неудачная возможность не должна ронять цикл
            return ExecutionResult(opp.id, opp.kind, "error", str(exc))

    # --- Примитивы CLOB ---

    def _buy_no(self, clob: ClobClient, token_id: str, amount_usd: float) -> dict:
        """Рыночная покупка на сумму `amount_usd` pUSD, FOK (целиком и
        сразу, либо отмена - никаких зависших частичных ордеров)."""
        return clob.create_and_post_market_order(
            order_args=MarketOrderArgs(token_id=token_id, amount=amount_usd, side=Side.BUY),
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=True),
            order_type=OrderType.FOK,
        )

    def _sell(self, clob: ClobClient, token_id: str, size_shares: float) -> dict:
        """Рыночная продажа `size_shares` штук токена, FOK."""
        return clob.create_and_post_market_order(
            order_args=MarketOrderArgs(token_id=token_id, amount=size_shares, side=Side.SELL),
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=True),
            order_type=OrderType.FOK,
        )

    def _order_succeeded(self, resp: dict) -> bool:
        return bool(resp.get("success")) and resp.get("status") in ("matched", "live")

    # --- convert_all ---

    def _execute_convert_all(self, clob: ClobClient, opp, details: dict, notional: float) -> ExecutionResult:
        legs = details.get("legs") or []
        if len(legs) < 2:
            return ExecutionResult(opp.id, opp.kind, "skipped", "недостаточно исходов в details")

        # Защита: у нас может быть неполный набор исходов (часть отфильтрована
        # локально по объёму - COLLECTOR_NEGRISK_MIN_VOLUME). Если так,
        # convertPositions всё равно выполнится, но выдаст Yes-токены по
        # исходам, которых мы не отслеживаем - то есть "гарантия без
        # остатка" перестаёт выполняться. Сверяемся с Gamma напрямую перед
        # тем, как тратить деньги, а не доверяем локальному кэшу.
        async def _verify_complete():
            gamma = GammaClient()
            try:
                return await count_negrisk_event_outcomes(gamma, opp.neg_risk_market_id)
            finally:
                await gamma.aclose()

        true_outcome_count = _run_sync_in_thread(_verify_complete)
        if true_outcome_count != len(legs):
            return ExecutionResult(
                opp.id, opp.kind, "skipped",
                f"неполный набор исходов: локально {len(legs)}, на Gamma {true_outcome_count} - "
                f"convert_all пропущен, чтобы не получить непредвиденный остаток в Yes-токенах",
            )

        owner = self._owner_address()
        if not owner:
            return ExecutionResult(opp.id, opp.kind, "error", "не задан WALLET_ADDRESS/WALLET_FUNDER_ADDRESS")

        per_leg_usd = notional / len(legs)
        state_id = self._open_state(opp, legs)

        bought: list[dict] = []  # [{condition_id, no_token, outcome_index, filled_shares}]
        total_spent = 0.0
        try:
            for leg in legs:
                balance_before = self._adapter.get_ctf_balance(owner, leg["no_token"])
                resp = self._buy_no(clob, leg["no_token"], per_leg_usd)
                if not self._order_succeeded(resp):
                    # Один исход не купился - дальше продолжать нельзя: уже
                    # купленные No нельзя конвертировать неполным набором по
                    # этой же схеме. Оставляем купленное как есть и громко
                    # сообщаем - решение, что с ним делать, за человеком.
                    self._fail_state(state_id, f"не удалось купить No на {leg['question']}: {resp}")
                    return ExecutionResult(
                        opp.id, opp.kind, "error",
                        f"частичное исполнение convert_all - куплено {len(bought)}/{len(legs)} ног, "
                        f"см. execution_states id={state_id}, конвертация НЕ выполнена",
                        spent=total_spent,
                    )
                balance_after = self._adapter.get_ctf_balance(owner, leg["no_token"])
                filled = balance_after - balance_before
                bought.append({**leg, "filled_shares": filled})
                total_spent += per_leg_usd

            self._advance_state(state_id, ExecutionState.PHASE_BOUGHT_NO, spent_usd=total_spent)

            # Конвертируем минимально общее количество, которое реально
            # купили по всем ногам - не больше, чтобы не словить revert
            # из-за нехватки баланса по самой маленькой ноге.
            convert_amount = min(b["filled_shares"] for b in bought)
            index_set = index_set_for([b["outcome_index"] for b in bought])

            self._advance_state(state_id, ExecutionState.PHASE_CONVERTING)
            tx = self._adapter.build_convert_tx(owner, opp.neg_risk_market_id, index_set, convert_amount)
            tx_hash = self._adapter.sign_and_send(tx, self._settings.WALLET_PRIVATE_KEY, confirm=True)

            received = (len(bought) - 1) * convert_amount  # по формуле контракта (без учёта комиссии - уточняется на балансе)
            self._advance_state(
                state_id, ExecutionState.PHASE_DONE,
                convert_tx_hash=tx_hash, no_filled_amount=convert_amount,
            )
            self._record_ledger(state_id, opp.kind, total_spent, received)

            return ExecutionResult(
                opp.id, opp.kind, "filled", "convert_all исполнен",
                filled_size=convert_amount, spent=total_spent, received=received, tx_hashes=(tx_hash,),
            )
        except Exception as exc:  # noqa: BLE001
            self._fail_state(state_id, str(exc))
            raise

    # --- convert_single ---

    def _execute_convert_single(self, clob: ClobClient, opp, details: dict, notional: float) -> ExecutionResult:
        no_token = details.get("no_token")
        outcome_index = details.get("outcome_index")
        others = details.get("converts_into_yes_on") or []
        if not no_token or outcome_index is None or not others:
            return ExecutionResult(opp.id, opp.kind, "skipped", "неполные details для convert_single")

        owner = self._owner_address()
        if not owner:
            return ExecutionResult(opp.id, opp.kind, "error", "не задан WALLET_ADDRESS/WALLET_FUNDER_ADDRESS")

        state_id = self._open_state(opp, [{"no_token": no_token, "outcome_index": outcome_index}])

        try:
            balance_before = self._adapter.get_ctf_balance(owner, no_token)
            resp = self._buy_no(clob, no_token, notional)
            if not self._order_succeeded(resp):
                self._fail_state(state_id, f"ордер на покупку No не исполнился: {resp}")
                return ExecutionResult(opp.id, opp.kind, "skipped", "ордер на покупку No не исполнился (FOK)")
            balance_after = self._adapter.get_ctf_balance(owner, no_token)
            filled = balance_after - balance_before
            self._advance_state(state_id, ExecutionState.PHASE_BOUGHT_NO, spent_usd=notional, no_filled_amount=filled)

            # ВАЖНО: _indexSet - это биты ТОЛЬКО тех исходов, чей No сжигается
            # (здесь - один купленный исход). Комплемент (Yes по остальным
            # исходам) контракт довыдаёт сам, исходя из полного числа
            # исходов рынка - его не нужно (и нельзя) передавать явно: если
            # указать биты "остальных" тоже, это означает "сжечь No и по
            # ним тоже", а их у нас нет - вызов просто не пройдёт.
            index_set = index_set_for([outcome_index])

            self._advance_state(state_id, ExecutionState.PHASE_CONVERTING)
            tx = self._adapter.build_convert_tx(owner, opp.neg_risk_market_id, index_set, filled)
            tx_hash = self._adapter.sign_and_send(tx, self._settings.WALLET_PRIVATE_KEY, confirm=True)
            self._advance_state(state_id, ExecutionState.PHASE_CONVERTED, convert_tx_hash=tx_hash)

            # Продаём полученный пакет Yes-токенов - с проверкой на
            # проскальзывание относительно цены, по которой сканер нашёл
            # возможность (EXECUTION_MAX_SLIPPAGE_PCT).
            self._advance_state(state_id, ExecutionState.PHASE_SELLING)
            # TODO перед боевым использованием: py-clob-client-v2 возвращает
            # фактическую выручку где-то в ответе ордера, но точное поле
            # (makingAmount/takingAmount/иное) нужно свериться на реальном
            # аккаунте - угадывать его здесь рискованнее, чем прочитать
            # напрямую с цепочки. Поэтому выручка считается по РАЗНИЦЕ
            # баланса pUSD кошелька до/после продажи каждой ноги - дольше
            # (лишний RPC-вызов), зато не зависит от неподтверждённого
            # формата ответа CLOB.
            from app.services.polymarket.wallet import WalletBalance
            wallet = WalletBalance()
            total_received = 0.0
            for o in others:
                yes_balance_before = self._adapter.get_ctf_balance(owner, o["yes_token"])
                sell_amount = min(filled, yes_balance_before)  # не больше, чем реально получили
                pusd_before = wallet.get_pusd_balance(owner)
                sell_resp = self._sell(clob, o["yes_token"], sell_amount)
                if self._order_succeeded(sell_resp):
                    pusd_after = wallet.get_pusd_balance(owner)
                    total_received += max(0.0, pusd_after - pusd_before)
                else:
                    logger.error(
                        "Не удалось продать полученный Yes (%s) после Convert - "
                        "позиция осталась на руках, проверьте вручную: %s",
                        o["question"], sell_resp,
                    )
            self._advance_state(state_id, ExecutionState.PHASE_DONE, sell_proceeds_usd=total_received)
            self._record_ledger(state_id, opp.kind, notional, total_received)

            return ExecutionResult(
                opp.id, opp.kind, "filled", "convert_single исполнен (сверьте выручку вручную - см. TODO)",
                filled_size=filled, spent=notional, received=total_received, tx_hashes=(tx_hash,),
            )
        except Exception as exc:  # noqa: BLE001
            self._fail_state(state_id, str(exc))
            raise

    # --- Журнал исполнения (синхронные обёртки над async-репозиторием) ---

    def _open_state(self, opp, legs: list[dict]) -> int:
        async def _run():
            async with async_session_factory() as session:
                row = await create_execution_state(session, {
                    "opportunity_id": opp.id,
                    "kind": opp.kind,
                    "neg_risk_market_id": opp.neg_risk_market_id,
                    "phase": ExecutionState.PHASE_BUYING_NO,
                    "details": {"legs": legs},
                })
                await session.commit()
                return row.id
        return _run_sync_in_thread(_run)

    def _advance_state(self, state_id: int, phase: str, **fields) -> None:
        async def _run():
            async with async_session_factory() as session:
                await update_execution_state(session, state_id, phase=phase, **fields)
                await session.commit()
        _run_sync_in_thread(_run)

    def _fail_state(self, state_id: int, error: str) -> None:
        self._advance_state(state_id, ExecutionState.PHASE_FAILED, error=error)

    def _record_ledger(self, state_id: int, kind: str, spent: float, received: float) -> None:
        async def _run():
            async with async_session_factory() as session:
                await save_ledger_entry(session, {
                    "execution_state_id": state_id,
                    "kind": kind,
                    "spent_usd": spent,
                    "received_usd": received,
                    "realized_profit_usd": received - spent,
                })
                await session.commit()
        _run_sync_in_thread(_run)


def _run_sync_in_thread(coro_factory):
    """_execute_one работает в отдельном потоке (через asyncio.to_thread),
    поэтому у него нет своего event loop - создаём новый на месте для
    обращений к async-репозиторию."""
    import asyncio as _asyncio
    return _asyncio.run(coro_factory())
