import logging
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import get_settings

logger = logging.getLogger(__name__)


def _retry_on_transient() -> Any:
    return retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
    )


class ClobClient:
    """Клиент CLOB API только для чтения (стаканы, цены).
    Публичное чтение, авторизация не нужна - для размещения ордеров
    понадобится отдельный аутентифицированный клиент (креды уровня L2),
    см. services/execution.py."""

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        settings = get_settings()
        self._base_url = settings.CLOB_API_URL
        self._client = client or httpx.AsyncClient(base_url=self._base_url, timeout=10)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @_retry_on_transient()
    async def get_order_book(self, token_id: str) -> dict:
        resp = await self._client.get("/book", params={"token_id": token_id})
        resp.raise_for_status()
        return resp.json()

    @_retry_on_transient()
    async def get_midpoint(self, token_id: str) -> float | None:
        resp = await self._client.get("/midpoint", params={"token_id": token_id})
        resp.raise_for_status()
        data = resp.json()
        try:
            return float(data["mid"])
        except (KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def to_snapshot_row(token_id: str, condition_id: str, book: dict) -> dict:
        """Превращает сырой ответ /book в плоские колонки для хранения,
        сохраняя при этом полный исходный payload - на случай того, что
        не разобрали."""
        bids = book.get("bids") or []
        asks = book.get("asks") or []

        # Polymarket отдаёт bids по возрастанию, asks тоже по возрастанию -
        # лучший bid последний (самая высокая цена), лучший ask первый (самая низкая).
        best_bid = _safe_float(bids[-1]["price"]) if bids else None
        best_bid_size = _safe_float(bids[-1]["size"]) if bids else None
        best_ask = _safe_float(asks[0]["price"]) if asks else None
        best_ask_size = _safe_float(asks[0]["size"]) if asks else None

        midpoint = None
        if best_bid is not None and best_ask is not None:
            midpoint = round((best_bid + best_ask) / 2, 6)

        return {
            "token_id": token_id,
            "condition_id": condition_id,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_size": best_bid_size,
            "best_ask_size": best_ask_size,
            "midpoint": midpoint,
            "tick_size": _safe_float(book.get("tick_size")),
            "neg_risk": book.get("neg_risk"),
            "raw_book": book,
        }


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
