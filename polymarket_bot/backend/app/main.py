import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.config import get_settings
from app.core.logging import setup_hang_watchdog, setup_logging
from app.repository.database import async_session_factory, init_db
from app.repository.market_repo import get_unfinished_execution_states
from app.services.collector import Collector
from app.services.execution import Executor
from app.services.strategy.negrisk_scanner import NegRiskScanner

settings = get_settings()
setup_logging(
    settings.LOG_LEVEL,
    log_file=settings.LOG_FILE,
    max_bytes=settings.LOG_FILE_MAX_BYTES,
    backup_count=settings.LOG_FILE_BACKUP_COUNT,
)
logger = logging.getLogger(__name__)


async def _check_unfinished_executions(notify) -> None:
    """При старте проверяет, не осталось ли в БД записей об исполнении,
    прерванных прошлым запуском (упал посреди Convert/продажи). Если
    есть - громко предупреждает (лог + Telegram, если бот запущен), но
    НЕ пытается доиграть их автоматически: решать, что делать с
    зависшей позицией, должен человек."""
    async with async_session_factory() as session:
        unfinished = await get_unfinished_execution_states(session)
    if not unfinished:
        return
    msg = (
        f"⚠️ Найдено {len(unfinished)} незавершённых исполнений с прошлого "
        f"запуска (возможно, процесс упал посреди Convert/продажи). "
        f"Проверьте таблицу execution_states вручную перед тем, как "
        f"включать ENABLE_ONCHAIN_EXECUTION."
    )
    logger.critical(msg)
    if notify is not None:
        await notify(msg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Диагностика на случай полного зависания процесса (включая сам
    # FastAPI) - если event loop перестанет отвечать дольше таймаута,
    # отдельный C-уровневый поток сам сбросит стек-трейсы всех потоков в
    # <LOG_FILE>_hang_dump.log, не дожидаясь, пока кто-то заметит проблему.
    if settings.HANG_WATCHDOG_TIMEOUT_SECONDS > 0:
        setup_hang_watchdog(settings.LOG_FILE, timeout_seconds=settings.HANG_WATCHDOG_TIMEOUT_SECONDS)

    await init_db()

    tasks: list[asyncio.Task] = []
    workers: dict[str, object] = {}

    if settings.RUN_COLLECTOR_IN_PROCESS:
        collector = Collector()
        workers["collector"] = collector
        tasks.append(asyncio.create_task(collector.run_forever()))
        logger.info("Фоновая задача сборщика данных запущена")
    else:
        workers["collector"] = None
        logger.info(
            "RUN_COLLECTOR_IN_PROCESS=false — запускайте сборщик отдельно: "
            "python -m app.services.collector"
        )

    if settings.RUN_SCANNER_IN_PROCESS:
        scanner = NegRiskScanner()
        workers["scanner"] = scanner
        tasks.append(asyncio.create_task(scanner.run_forever()))
        logger.info("Фоновая задача сканера NegRisk запущена")
    else:
        workers["scanner"] = None
        logger.info(
            "RUN_SCANNER_IN_PROCESS=false — запускайте сканер отдельно: "
            "python -m app.services.strategy.negrisk_scanner"
        )

    bot = None
    bot_task: asyncio.Task | None = None
    notify = None
    if settings.RUN_TELEGRAM_BOT_IN_PROCESS and settings.TELEGRAM_BOT_TOKEN:
        from app.bot.telegram_bot import build_bot, build_dispatcher, notify_admins

        bot = build_bot(settings)
        dp = build_dispatcher()
        bot._polymarket_state = workers  # видно хендлерам в telegram_bot.py
        bot_task = asyncio.create_task(dp.start_polling(bot))
        tasks.append(bot_task)
        logger.info("Фоновая задача Telegram-бота запущена")

        async def notify(text: str) -> None:  # noqa: F811 - переопределяем намеренно
            await notify_admins(bot, settings, text)
    else:
        logger.info(
            "Telegram-бот не запущен (RUN_TELEGRAM_BOT_IN_PROCESS=false или "
            "не задан TELEGRAM_BOT_TOKEN) — управление и алерты недоступны."
        )

    await _check_unfinished_executions(notify)

    if settings.RUN_EXECUTOR_IN_PROCESS:
        executor = Executor()
        workers["executor"] = executor
        tasks.append(asyncio.create_task(executor.run_forever()))
        logger.info(
            "Фоновая задача исполнителя запущена (ENABLE_ONCHAIN_EXECUTION=%s)",
            settings.ENABLE_ONCHAIN_EXECUTION,
        )
    else:
        workers["executor"] = None
        logger.info("RUN_EXECUTOR_IN_PROCESS=false — сделки совершаться не будут.")

    app.state.workers = workers

    yield

    for name in ("collector", "scanner", "executor"):
        worker = workers.get(name)
        if worker is not None and hasattr(worker, "stop"):
            worker.stop()
    if bot is not None:
        await bot.session.close()
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    if tasks:
        logger.info("Фоновые задачи остановлены")


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
app.include_router(api_router, prefix="/api/v1")
