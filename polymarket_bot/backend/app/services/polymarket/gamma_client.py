import json
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


class GammaClient:
    """Клиент Gamma API только для чтения (поиск рынков/событий).
    Публичный эндпоинт, авторизация не нужна."""

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        settings = get_settings()
        self._base_url = settings.GAMMA_API_URL
        self._client = client or httpx.AsyncClient(base_url=self._base_url, timeout=10)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @_retry_on_transient()
    async def list_markets(
        self,
        *,
        active: bool = True,
        closed: bool = False,
        limit: int = 50,
        offset: int = 0,
        order: str = "volume",
        ascending: bool = False,
        neg_risk: bool | None = None,
    ) -> list[dict]:
        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
            "order": order,
            "ascending": str(ascending).lower(),
        }
        if neg_risk is not None:
            # Фильтр по самому Gamma, а не наше постфильтрование - иначе
            # придётся пролистать весь каталог рынков, чтобы найти NegRisk.
            params["neg_risk"] = str(neg_risk).lower()
        resp = await self._client.get("/markets", params=params)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def to_normalized_market(raw: dict) -> dict | None:
        """Gamma отдаёт outcomes/outcomePrices/clobTokenIds как
        JSON-строки, а не массивы. Разбираем их и отбрасываем рынки,
        которыми нельзя торговать (нет clobTokenIds => нет стакана,
        который можно опрашивать)."""
        condition_id = raw.get("conditionId")
        if not condition_id:
            return None

        def _parse_list(field: str) -> list | None:
            value = raw.get(field)
            if not value:
                return None
            try:
                return json.loads(value) if isinstance(value, str) else value
            except (json.JSONDecodeError, TypeError):
                logger.warning("Не удалось разобрать поле %s для рынка %s", field, condition_id)
                return None

        clob_token_ids = _parse_list("clobTokenIds")
        if not clob_token_ids:
            return None

        return {
            "condition_id": condition_id,
            "question": raw.get("question", ""),
            "slug": raw.get("slug"),
            "category": raw.get("category"),
            "active": bool(raw.get("active", False)),
            "closed": bool(raw.get("closed", False)),
            "volume": _safe_float(raw.get("volume")),
            "liquidity": _safe_float(raw.get("liquidity")),
            "outcomes": _parse_list("outcomes"),
            "outcome_prices": _parse_list("outcomePrices"),
            "clob_token_ids": clob_token_ids,
            "end_date": raw.get("endDate"),
            "neg_risk": bool(raw.get("negRisk", False)),
            "neg_risk_market_id": raw.get("negRiskMarketID"),
            "neg_risk_augmented": bool(raw.get("negRiskAugmented", False)),
            # Битовый индекс исхода внутри NegRisk-рынка - нужен, чтобы
            # построить корректный _indexSet для convertPositions. Подтверждено
            # напрямую по ответу Gamma: questionID = negRiskMarketID с
            # заменённым последним байтом на этот индекс, groupItemThreshold
            # даёт то же число напрямую строкой. Не позиция в нашем списке -
            # porядок, в котором Gamma возвращает рынки, не гарантированно
            # совпадает с индексом на контракте.
            "neg_risk_outcome_index": _safe_int(raw.get("groupItemThreshold")),
        }


async def count_negrisk_event_outcomes(gamma: "GammaClient", neg_risk_market_id: str) -> int:
    """Сколько исходов НА САМОМ ДЕЛЕ у этого NegRisk-события по данным
    Gamma - включая те, что отфильтрованы локально по объёму
    (COLLECTOR_NEGRISK_MIN_VOLUME) и потому не попали в нашу БД. Нужно
    перед исполнением convert_all: контракт выдаёт Yes-токены по ВСЕМ
    исходам, не входящим в наш `_indexSet`, включая те, о которых мы
    локально не знаем - если наш набор "ног" неполон, гарантия
    "никакого остатка после convert_all" не выполняется."""
    total = 0
    offset = 0
    page_size = 100
    for _ in range(20):  # тот же защитный потолок страниц, что и в Collector
        page = await gamma.list_markets(
            active=True, closed=False, neg_risk=True, limit=page_size, offset=offset,
            order="volume", ascending=False,
        )
        if not page:
            break
        for raw in page:
            if raw.get("negRiskMarketID") == neg_risk_market_id:
                total += 1
        if len(page) < page_size:
            break
        offset += page_size
    return total


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None
