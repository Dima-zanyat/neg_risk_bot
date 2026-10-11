import logging
from collections.abc import AsyncIterator
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings
from app.repository.models import Base

logger = logging.getLogger(__name__)
settings = get_settings()


def _ensure_sqlite_dir(database_url: str) -> None:
    """SQLite сам не создаёт родительскую папку. У Postgres и других
    СУБД такой проблемы нет, для них это ничего не делает."""
    if not database_url.startswith("sqlite"):
        return
    # например "sqlite+aiosqlite:///./data/polymarket.db" -> "./data/polymarket.db"
    path_part = database_url.split("///", 1)[-1]
    db_path = Path(path_part)
    if db_path.parent != Path("."):
        db_path.parent.mkdir(parents=True, exist_ok=True)


_ensure_sqlite_dir(settings.DATABASE_URL)

# echo=False намеренно - включите True локально, если нужны сырые логи SQL.
engine = create_async_engine(settings.DATABASE_URL, echo=False)


if settings.DATABASE_URL.startswith("sqlite"):
    # По умолчанию SQLite на запись блокирует файл целиком, и чем больше
    # транзакция (а upsert нескольких тысяч рынков за цикл - как раз
    # большая транзакция), тем дольше окно блокировки. В этом проекте
    # Collector, Scanner, Executor и Telegram-бот - четыре независимые
    # asyncio-задачи, все лезущие в один файл БД одновременно; без WAL
    # это со временем превращается в "зависания", которые усиливаются
    # по мере роста числа записей (длиннее транзакция -> дольше блокировка).
    # WAL-режим даёт читателям работать, пока идёт запись, а busy_timeout
    # заставляет ждать и повторить попытку вместо немедленной ошибки
    # "database is locked".
    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record) -> None:  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=10000")  # ждать до 10с при конфликте, не падать сразу
        cursor.execute("PRAGMA synchronous=NORMAL")  # безопасно вместе с WAL, быстрее чем FULL
        cursor.close()
        logger.debug("SQLite: включены WAL, busy_timeout=10000ms, synchronous=NORMAL")

async_session_factory = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Создаёт таблицы, если их ещё нет. Годится для текущего этапа;
    когда схема стабилизируется - стоит перейти на миграции Alembic."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with async_session_factory() as session:
        yield session
