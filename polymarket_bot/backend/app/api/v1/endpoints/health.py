from fastapi import APIRouter, Request

from app.config import get_settings
from app.services.polymarket.wallet import get_current_bankroll_async

router = APIRouter()


def _worker_status(worker) -> dict:
    if worker is None:
        return {"status": "not_running", "last_cycle_at": None, "last_error": None}
    return {
        "status": "error" if worker.last_cycle_error else "ok",
        "last_cycle_at": worker.last_cycle_at,
        "last_error": worker.last_cycle_error,
    }


@router.get("/health")
async def health(request: Request) -> dict:
    """Проверка живости/статуса - показывает, реально ли продвигаются
    сборщик, сканер и исполнитель, а не просто "процесс запущен" (процесс
    может быть жив, пока его фоновые циклы молча зависли или падают
    каждый раз)."""
    settings = get_settings()
    workers = getattr(request.app.state, "workers", {})
    collector = workers.get("collector")
    scanner = workers.get("scanner")
    executor = workers.get("executor")

    result = {
        "status": "ok",
        "collector": _worker_status(collector),
        "scanner": {
            **_worker_status(scanner),
            "last_opportunity_count": getattr(scanner, "last_opportunity_count", None),
        },
        "executor": {
            **_worker_status(executor),
            "enabled": settings.ENABLE_ONCHAIN_EXECUTION,
            "paused": getattr(executor, "is_paused", None) if executor else None,
        },
        "bankroll_usd": await get_current_bankroll_async(timeout=8),
    }
    return result
