import asyncio
import faulthandler
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(level: str = "INFO", log_file: str | None = None,
                   max_bytes: int = 10_000_000, backup_count: int = 5) -> None:
    """Настраивает корневой логгер один раз, при старте процесса.

    Пишет и в stdout (для просмотра вживую), и, если указан `log_file`, в
    файл с ротацией - важно для бота, который работает сутками на
    локальной машине без присмотра: если он упал или завис ночью, нужно
    посмотреть в лог-файл утром, а не в историю закрытого терминала.
    Ротация (max_bytes/backup_count) не даёт файлу логов расти
    бесконечно при непрерывной работе."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
        )

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,  # перезаписать конфигурацию, если basicConfig уже вызывался раньше
    )
    # Приглушаем болтливые логгеры сторонних библиотек.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("aiogram").setLevel(logging.WARNING)


def setup_hang_watchdog(log_file: str, timeout_seconds: int = 90) -> None:
    """Сторожевой таймер на зависание всего процесса (не отдельной
    asyncio-задачи - у faulthandler свой C-уровневый поток, не зависящий
    от event loop). Если `_rearm()` не вызывается вовремя - то есть
    event loop перестал вообще что-либо обрабатывать, что бы его ни
    заблокировало - через `timeout_seconds` в лог сбрасываются стек-трейсы
    ВСЕХ потоков процесса. Это и есть ответ на вопрос "на какой именно
    строке зависло", вместо дальнейших догадок по косвенным логам.

    Работает на Windows (в отличие от faulthandler.register(), которому
    нужны POSIX-сигналы) - dump_traceback_later использует отдельный
    поток-таймер на уровне C, а не signal handler.
    """
    dump_path = Path(log_file).with_name(Path(log_file).stem + "_hang_dump.log")
    # Файл открывается ОДИН раз и переиспользуется на каждом перевзводе -
    # открытие нового файла на каждый тик heartbeat (как было раньше)
    # медленно копило утечку файловых дескрипторов на многодневной работе.
    dump_file = open(dump_path, "a", encoding="utf-8")

    def _rearm() -> None:
        faulthandler.cancel_dump_traceback_later()
        faulthandler.dump_traceback_later(timeout_seconds, repeat=False, file=dump_file)

    async def _heartbeat() -> None:
        # Явный запас, а не просто "меньше таймаута": при интервале,
        # близком к самому таймауту, обычный джиттер планировщика asyncio
        # (открытие файла, другие задачи и т.п.) может сработать позже,
        # чем успеет исполниться таймер - ложное срабатывание. Запас в
        # 3 раза проверен тестом на реальном значении (90с -> интервал 30с).
        interval = max(1, timeout_seconds // 3)
        while True:
            _rearm()
            await asyncio.sleep(interval)

    _rearm()
    asyncio.create_task(_heartbeat())
    logging.getLogger(__name__).info(
        "Сторожевой таймер на зависание процесса включён (таймаут %dс, дамп стеков в %s)",
        timeout_seconds, dump_path,
    )
