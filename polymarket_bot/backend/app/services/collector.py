"""
Слой сбора данных.

Цикл:
  1. Берём топ-N активных рынков из Gamma API (по объёму).
  2. Обновляем их метаданные в `markets` (upsert).
  3. Для каждого токена-исхода рынка берём стакан из CLOB API и
     добавляем строку-снепшот в `order_book_snapshots`.

Можно запускать двумя способами без изменения кода:
  - В том же процессе, фоновой задачей asyncio из FastAPI (по умолчанию -
    см. app/main.py, управляется RUN_COLLECTOR_IN_PROCESS).
  - Отдельным процессом: `python -m app.services.collector`. Пригодится,
    если разнести сбор данных и торговлю на разные процессы под
    отдельным supervisor'ом ради отказоустойчивости.
"""

import asyncio
import logging
from datetime import datetime, timezone

from app.config import get_settings
from app.core.logging import setup_logging
from app.repository.database import async_session_factory, init_db
from app.repository.market_repo import prune_old_snapshots, save_order_book_snapshot, upsert_market
from app.services.polymarket.clob_client import ClobClient
from app.services.polymarket.gamma_client import GammaClient

logger = logging.getLogger(__name__)


class Collector:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._stop_event = asyncio.Event()
        self.last_cycle_at: datetime | None = None
        self.last_cycle_error: str | None = None
        self._cycle_count = 0

    def stop(self) -> None:
        self._stop_event.set()

    async def run_forever(self) -> None:
        logger.info(
            "Сборщик запущен (interval=%sс, market_limit=%s)",
            self._settings.COLLECTOR_INTERVAL_SECONDS,
            self._settings.COLLECTOR_MARKET_LIMIT,
        )
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self.run_once(), timeout=self._settings.COLLECTOR_CYCLE_TIMEOUT_SECONDS
                )
                self._cycle_count += 1
                if self._cycle_count % self._settings.COLLECTOR_PRUNE_EVERY_N_CYCLES == 0:
                    await self._prune_snapshots()
                self.last_cycle_error = None
            except asyncio.TimeoutError:
                self.last_cycle_error = (
                    f"цикл превысил COLLECTOR_CYCLE_TIMEOUT_SECONDS="
                    f"{self._settings.COLLECTOR_CYCLE_TIMEOUT_SECONDS}с"
                )
                logger.warning(
                    "Цикл сборщика прерван по общему таймауту (%dс) - что-то зависло внутри "
                    "дольше таймаутов отдельных запросов, продолжаем со следующей итерации",
                    self._settings.COLLECTOR_CYCLE_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001 - сборщик не должен умирать
                self.last_cycle_error = str(exc)
                logger.exception("Цикл сборщика упал, повторим на следующей итерации")
            self.last_cycle_at = datetime.now(timezone.utc)

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._settings.COLLECTOR_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass  # нормально: просто значит, что пора следующему циклу

    async def _prune_snapshots(self) -> None:
        async with async_session_factory() as session:
            deleted = await prune_old_snapshots(
                session, older_than_hours=self._settings.COLLECTOR_SNAPSHOT_RETENTION_HOURS
            )
            await session.commit()
        if deleted:
            logger.info("Удалено %d устаревших снепшотов стакана (старше %dч)",
                        deleted, self._settings.COLLECTOR_SNAPSHOT_RETENTION_HOURS)

    async def run_once(self) -> None:
        gamma = GammaClient()
        clob = ClobClient()
        try:
            general_markets = await self._collect_markets(gamma)
            negrisk_markets = await self._collect_all_negrisk_markets(gamma)
            # Объединяем по condition_id - NegRisk-рынок мог попасть в оба
            # списка (он же может быть в топе по объёму), не дублируем.
            by_condition_id = {m["condition_id"]: m for m in general_markets}
            by_condition_id.update({m["condition_id"]: m for m in negrisk_markets})
            await self._collect_order_books(clob, list(by_condition_id.values()))
        finally:
            await gamma.aclose()
            await clob.aclose()

    async def _collect_markets(self, gamma: GammaClient) -> list[dict]:
        raw_markets = await asyncio.wait_for(
            gamma.list_markets(
                active=True,
                closed=False,
                limit=self._settings.COLLECTOR_MARKET_LIMIT,
                order="volume",
                ascending=False,
            ),
            timeout=self._settings.COLLECTOR_ORDERBOOK_REQUEST_TIMEOUT_SECONDS,
        )

        normalized = [
            m for m in (GammaClient.to_normalized_market(r) for r in raw_markets) if m
        ]

        async with async_session_factory() as session:
            for market in normalized:
                await upsert_market(session, market)
            await session.commit()

        logger.info("Собрано %d/%d рынков (топ по объёму)", len(normalized), len(raw_markets))
        return normalized

    async def _collect_all_negrisk_markets(self, gamma: GammaClient) -> list[dict]:
        """Листает Gamma постранично с фильтром neg_risk=true, чтобы
        получить ВСЕ активные исходы ВСЕХ NegRisk-событий - в отличие от
        _collect_markets (топ-N по объёму), где у многоисходного события
        обычно оказываются собраны не все исходы. Без этого сканер не
        может оценить большинство событий целиком."""
        page_size = self._settings.COLLECTOR_NEGRISK_PAGE_SIZE
        all_normalized: list[dict] = []
        offset = 0

        for page_num in range(self._settings.COLLECTOR_NEGRISK_MAX_PAGES):
            try:
                raw_page = await asyncio.wait_for(
                    gamma.list_markets(
                        active=True,
                        closed=False,
                        neg_risk=True,
                        limit=page_size,
                        offset=offset,
                        order="volume",
                        ascending=False,
                    ),
                    timeout=self._settings.COLLECTOR_ORDERBOOK_REQUEST_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                # Одна зависшая страница не должна стоить всего цикла -
                # используем то, что уже успели собрать на предыдущих.
                logger.warning(
                    "Страница %d сбора NegRisk-рынков не ответила за %dс - "
                    "останавливаю пагинацию, использую уже собранное (%d рынков)",
                    page_num, self._settings.COLLECTOR_ORDERBOOK_REQUEST_TIMEOUT_SECONDS,
                    len(all_normalized),
                )
                break
            if not raw_page:
                break
            # Фильтр по объёму здесь НЕ применяется намеренно: он резал бы
            # отдельные исходы, а не события целиком - у события с одним
            # фаворитом и россыпью аутсайдеров почти без объёма это
            # оставляло бы от события 1 исход вместо всех, и сканер не мог
            # бы его оценить вообще. Решение, какие СОБЫТИЯ стоит
            # отслеживать по объёму, принимается позже, в
            # _collect_order_books - там же, где объём учитывается
            # правильно: по событию целиком, не по отдельному исходу.
            all_normalized.extend(
                m for m in (GammaClient.to_normalized_market(r) for r in raw_page) if m
            )
            if len(raw_page) < page_size:
                break  # последняя страница
            offset += page_size

        async with async_session_factory() as session:
            for market in all_normalized:
                await upsert_market(session, market)
            await session.commit()

        logger.info("Собрано %d рынков NegRisk (все активные исходы, не только топ по объёму)", len(all_normalized))
        return all_normalized

    async def _collect_order_books(self, clob: ClobClient, markets: list[dict]) -> None:
        s = self._settings

        # Приоритет и потолок - НА УРОВНЕ СОБЫТИЯ ЦЕЛИКОМ, не отдельного
        # токена. Если резать список токенов по жёсткому числу без учёта
        # того, что несколько токенов принадлежат одному NegRisk-событию,
        # событие легко разрезать пополам - часть исходов попадёт в цикл,
        # часть нет, и сканер не сможет оценить событие целиком (ровно
        # так появился баг с "1 исход вместо 5" на предыдущем шаге).
        # Поэтому: группируем по neg_risk_market_id (рынки без него - сами
        # себе группа из одного), приоритет по суммарному объёму группы,
        # и либо берём ВСЕ токены группы, либо не берём её вообще.
        groups: dict[str, list[dict]] = {}
        for market in markets:
            key = market.get("neg_risk_market_id") or f"__single__{market['condition_id']}"
            groups.setdefault(key, []).append(market)

        def group_volume(group: list[dict]) -> float:
            return sum(m.get("volume") or 0 for m in group)

        sorted_groups = sorted(groups.values(), key=group_volume, reverse=True)

        pairs: list[tuple[str, str]] = []  # (condition_id, token_id)
        cap = s.COLLECTOR_MAX_ORDERBOOK_TOKENS_PER_CYCLE
        total_available = sum(len(m["clob_token_ids"]) for group in sorted_groups for m in group)
        skipped_events = 0

        for group in sorted_groups:
            group_pairs = [
                (m["condition_id"], token_id) for m in group for token_id in m["clob_token_ids"]
            ]
            if len(pairs) + len(group_pairs) > cap:
                skipped_events += 1
                continue  # не берём событие частично - пропускаем целиком до следующего цикла
            pairs.extend(group_pairs)

        if total_available > len(pairs):
            logger.warning(
                "Токенов больше, чем помещается в один цикл (%d из %d, пропущено целиком "
                "%d маловолюмных событий) - соберутся в следующих циклах, по убыванию объёма",
                len(pairs), total_available, skipped_events,
            )

        semaphore = asyncio.Semaphore(s.COLLECTOR_CONCURRENCY)
        snapshots: list[dict] = []
        errors = 0

        async def fetch_one(condition_id: str, token_id: str) -> None:
            nonlocal errors
            async with semaphore:
                try:
                    book = await asyncio.wait_for(
                        clob.get_order_book(token_id),
                        timeout=s.COLLECTOR_ORDERBOOK_REQUEST_TIMEOUT_SECONDS,
                    )
                except Exception:  # noqa: BLE001 - один плохой токен не должен рушить всю пачку
                    errors += 1
                    logger.debug("Не удалось получить стакан для токена %s", token_id, exc_info=True)
                    return
                snapshots.append(ClobClient.to_snapshot_row(token_id, condition_id, book))

        tasks = [fetch_one(condition_id, token_id) for condition_id, token_id in pairs]
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks), timeout=s.COLLECTOR_ORDERBOOK_PHASE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Сбор стаканов прерван по таймауту фазы (%dс) - сохраняю %d/%d успевших",
                s.COLLECTOR_ORDERBOOK_PHASE_TIMEOUT_SECONDS, len(snapshots), len(pairs),
            )

        # Сохраняем то, что успели собрать, даже если сработал таймаут фазы -
        # частичный прогресс лучше, чем полная потеря цикла.
        async with async_session_factory() as session:
            for snapshot in snapshots:
                await save_order_book_snapshot(session, snapshot)
            await session.commit()

        logger.info("Собрано %d снепшотов стакана из %d токенов (ошибок: %d)", len(snapshots), len(pairs), errors)


async def _main() -> None:
    settings = get_settings()
    setup_logging(settings.LOG_LEVEL, log_file=settings.LOG_FILE,
                  max_bytes=settings.LOG_FILE_MAX_BYTES, backup_count=settings.LOG_FILE_BACKUP_COUNT)
    await init_db()
    collector = Collector()
    await collector.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
