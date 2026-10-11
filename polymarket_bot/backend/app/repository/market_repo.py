import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.repository.models import (
    ExecutionLedger,
    ExecutionState,
    Market,
    NegRiskOpportunity,
    OrderBookSnapshot,
)

logger = logging.getLogger(__name__)


async def upsert_market(session: AsyncSession, market_data: dict) -> None:
    """Добавляет рынок или обновляет его, если condition_id уже есть.
    Учитывает диалект СУБД (SQLite для локальной разработки, Postgres -
    если переключитесь позже)."""

    values = {
        "condition_id": market_data["condition_id"],
        "question": market_data["question"],
        "slug": market_data.get("slug"),
        "category": market_data.get("category"),
        "active": market_data.get("active", False),
        "closed": market_data.get("closed", False),
        "volume": market_data.get("volume"),
        "liquidity": market_data.get("liquidity"),
        "outcomes": market_data.get("outcomes"),
        "outcome_prices": market_data.get("outcome_prices"),
        "clob_token_ids": market_data.get("clob_token_ids"),
        "end_date": market_data.get("end_date"),
        "neg_risk": market_data.get("neg_risk", False),
        "neg_risk_market_id": market_data.get("neg_risk_market_id"),
        "neg_risk_augmented": market_data.get("neg_risk_augmented", False),
        "neg_risk_outcome_index": market_data.get("neg_risk_outcome_index"),
    }

    dialect = session.bind.dialect.name if session.bind else "sqlite"
    insert_fn = pg_insert if dialect == "postgresql" else sqlite_insert

    stmt = insert_fn(Market).values(**values)
    update_cols = {k: v for k, v in values.items() if k != "condition_id"}
    stmt = stmt.on_conflict_do_update(index_elements=["condition_id"], set_=update_cols)

    await session.execute(stmt)


async def save_order_book_snapshot(session: AsyncSession, snapshot: dict) -> None:
    session.add(OrderBookSnapshot(**snapshot))


async def get_active_markets(session: AsyncSession) -> list[Market]:
    result = await session.execute(select(Market).where(Market.active.is_(True)))
    return list(result.scalars().all())


async def get_latest_snapshot(session: AsyncSession, token_id: str) -> OrderBookSnapshot | None:
    result = await session.execute(
        select(OrderBookSnapshot)
        .where(OrderBookSnapshot.token_id == token_id)
        .order_by(OrderBookSnapshot.captured_at.desc())
        .limit(1)
    )
    return result.scalars().first()


async def get_latest_snapshots_for_tokens(
    session: AsyncSession, token_ids: list[str], max_age_seconds: int = 300
) -> dict[str, OrderBookSnapshot]:
    """Последний снепшот по каждому token_id, без тех, что старше
    `max_age_seconds` (устаревший стакан хуже, чем его отсутствие - не
    давать сканеру оценивать возможность по данным десятиминутной
    давности).

    НАМЕРЕННО без `token_id IN (...)` в SQL: при нескольких тысячах
    активных NegRisk-токенов (а их бывает именно столько) такой список
    превращается в запрос с тысячами связанных параметров - компиляция
    и привязка такого запроса в sqlite3 синхронна и достаточно тяжела,
    чтобы из-за GIL на секунды-минуты застопорить весь процесс целиком
    (включая обработку HTTP в FastAPI в том же процессе), а вызывается
    это сканером каждые NEGRISK_SCAN_INTERVAL_SECONDS. Вместо этого
    фильтруем только по времени (дёшево, по индексу) и сверяем с нужными
    token_id уже в Python - set.__contains__ на тысячах строк ничтожно
    дёшев по сравнению с SQL-запросом на тысячи параметров."""
    if not token_ids:
        return {}
    wanted = set(token_ids)

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
    result = await session.execute(
        select(OrderBookSnapshot)
        .where(OrderBookSnapshot.captured_at >= cutoff)
        .order_by(OrderBookSnapshot.captured_at.desc())
    )
    latest: dict[str, OrderBookSnapshot] = {}
    for snap in result.scalars().all():
        if snap.token_id in wanted and snap.token_id not in latest:
            latest[snap.token_id] = snap  # первое совпадение = самое свежее
    return latest


async def prune_old_snapshots(session: AsyncSession, older_than_hours: int = 24) -> int:
    """Удаляет снепшоты стакана старше `older_than_hours`. Таблица
    append-only и растёт без ограничения; сканеру нужны только последние
    несколько минут (max_age_seconds в get_latest_snapshots_for_tokens),
    всё остальное - чистый балласт, который со временем замедляет
    запросы и раздувает файл БД. Старые данные для бэктеста, если нужны,
    стоит выгружать отдельно до чистки, а не держать вечно в рабочей БД."""
    from sqlalchemy import delete

    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    result = await session.execute(
        delete(OrderBookSnapshot).where(OrderBookSnapshot.captured_at < cutoff)
    )
    return result.rowcount or 0


async def get_active_negrisk_markets(session: AsyncSession) -> list[Market]:
    """Все активные рынки, относящиеся к NegRisk (многоисходному) событию,
    включая augmented/placeholder - решение, что с ними делать, принимает
    вызывающий код."""
    result = await session.execute(
        select(Market).where(
            Market.active.is_(True),
            Market.neg_risk.is_(True),
            Market.neg_risk_market_id.is_not(None),
        )
    )
    return list(result.scalars().all())


async def save_opportunity(session: AsyncSession, opportunity: dict) -> None:
    session.add(NegRiskOpportunity(**opportunity))


async def get_recent_opportunities(
    session: AsyncSession, limit: int = 50
) -> list[NegRiskOpportunity]:
    result = await session.execute(
        select(NegRiskOpportunity).order_by(NegRiskOpportunity.detected_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


async def get_opportunity_by_id(session: AsyncSession, opportunity_id: int) -> NegRiskOpportunity | None:
    return await session.get(NegRiskOpportunity, opportunity_id)


# --- Журнал исполнения (восстановление после сбоя) ---


async def create_execution_state(session: AsyncSession, state: dict) -> ExecutionState:
    row = ExecutionState(**state)
    session.add(row)
    await session.flush()  # чтобы получить row.id ещё до commit
    return row


async def update_execution_state(session: AsyncSession, state_id: int, **fields) -> None:
    row = await session.get(ExecutionState, state_id)
    if row is None:
        logger.warning("update_execution_state: запись %s не найдена", state_id)
        return
    for key, value in fields.items():
        setattr(row, key, value)


async def get_unfinished_execution_states(session: AsyncSession) -> list[ExecutionState]:
    """Записи, оставшиеся в незавершённой фазе - признак того, что
    предыдущий запуск прервался посреди последовательности Convert/продажа.
    Вызывается при старте Executor'а; при непустом результате процесс
    обязан сообщить об этом (в Telegram) и не продолжать автоматически."""
    result = await session.execute(
        select(ExecutionState).where(
            ExecutionState.phase.not_in([ExecutionState.PHASE_DONE, ExecutionState.PHASE_FAILED])
        )
    )
    return list(result.scalars().all())


async def save_ledger_entry(session: AsyncSession, entry: dict) -> None:
    session.add(ExecutionLedger(**entry))


async def get_total_realized_profit(session: AsyncSession) -> float:
    result = await session.execute(select(func.sum(ExecutionLedger.realized_profit_usd)))
    return result.scalar() or 0.0


async def get_recent_ledger_entries(session: AsyncSession, limit: int = 20) -> list[ExecutionLedger]:
    result = await session.execute(
        select(ExecutionLedger).order_by(ExecutionLedger.recorded_at.desc()).limit(limit)
    )
    return list(result.scalars().all())
