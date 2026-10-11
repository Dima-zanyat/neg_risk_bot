"""Небольшие independent-от-библиотек утилиты для обхода стакана заявок
с оценкой стоимости заполнения конкретного объёма - в отличие от взгляда
только на лучшую цену (top-of-book), которая переоценивает, сколько
реально можно исполнить по этой цене."""

from typing import Literal


def _safe_float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def walk_book(
    raw_book: dict, side: Literal["buy", "sell"], target_size: float
) -> tuple[float, float]:
    """Возвращает (стоимость_или_выручка, заполненный_объём) для
    заполнения `target_size` единиц с указанной стороны стакана.

    side="buy"  -> идём по `asks`, от самой дешёвой (Polymarket отдаёт
                   asks по возрастанию цены, так что порядок в списке уже
                   правильный).
    side="sell" -> идём по `bids`, от самой высокой (Polymarket отдаёт
                   bids по возрастанию, поэтому здесь список переворачивается).
    """
    if side == "buy":
        levels = raw_book.get("asks") or []
    else:
        levels = list(reversed(raw_book.get("bids") or []))

    remaining = target_size
    total = 0.0
    filled = 0.0

    for level in levels:
        if remaining <= 0:
            break
        price = _safe_float(level.get("price"))
        size = _safe_float(level.get("size"))
        if price is None or size is None or size <= 0:
            continue
        take = min(size, remaining)
        total += take * price
        filled += take
        remaining -= take

    return total, filled
