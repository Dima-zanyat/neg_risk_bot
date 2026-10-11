"""
Диагностика: почему сканер ничего не находит.

Запуск (из корня проекта, при активном venv):
    python scripts/diagnose.py

Показывает:
  1. Сколько рынков вообще собрано и сколько из них NegRisk.
  2. NegRisk-события, сгруппированные по negRiskMarketID, с числом
     собранных исходов в каждом - если у события 15 исходов, а собрано 2,
     сканер не может его оценить (нужны ВСЕ исходы с живым стаканом).
  3. У скольких событий есть свежие снепшоты стакана по ВСЕМ исходам
     (условие, при котором сканер вообще способен что-то посчитать).
  4. "Почти найденные" возможности - с минимальным порогом прибыли 0,
     чтобы видеть реальные текущие расхождения цен, а не только те, что
     уже прошли фильтр NEGRISK_MIN_PROFIT_PCT.
"""

import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from app.config import get_settings  # noqa: E402
from app.repository.database import async_session_factory  # noqa: E402
from app.repository.market_repo import (  # noqa: E402
    get_active_negrisk_markets,
    get_latest_snapshots_for_tokens,
)
from app.services.strategy.negrisk_scanner import NegRiskScanner  # noqa: E402


async def main() -> None:
    settings = get_settings()

    async with async_session_factory() as session:
        markets = await get_active_negrisk_markets(session)

        events: dict[str, list] = defaultdict(list)
        for m in markets:
            events[m.neg_risk_market_id].append(m)

        print(f"Всего активных NegRisk-рынков в БД: {len(markets)}")
        print(f"Сгруппировано в {len(events)} событий\n")

        all_token_ids = [
            t for m in markets for t in (m.clob_token_ids or [])
        ]
        books = await get_latest_snapshots_for_tokens(session, all_token_ids)

        complete_events = 0
        for market_id, legs in sorted(events.items(), key=lambda kv: -len(kv[1]))[:15]:
            with_index = sum(1 for leg in legs if leg.neg_risk_outcome_index is not None)
            with_book = sum(
                1 for leg in legs
                if leg.clob_token_ids
                and len(leg.clob_token_ids) >= 2
                and leg.clob_token_ids[0] in books
                and leg.clob_token_ids[1] in books
            )
            augmented = any(leg.neg_risk_augmented for leg in legs)
            status = "ПОЛНОЕ" if with_book == len(legs) and not augmented else "неполное"
            if status == "ПОЛНОЕ":
                complete_events += 1
            print(
                f"[{status}] {market_id[:16]}… — исходов: {len(legs)}, "
                f"с outcome_index: {with_index}, со свежим стаканом: {with_book}, "
                f"augmented: {augmented}"
            )
            if len(legs) <= 3:
                for leg in legs:
                    print(f"    - {leg.question[:70]}")

        print(f"\n(показаны 15 крупнейших по числу исходов событий)")
        print(f"Событий с полным набором живых данных (сканер способен их оценить): {complete_events}/{len(events)}\n")

        print("=" * 60)
        print("Поиск с порогом прибыли 0% (включая отрицательные) -")
        print("показывает реальные текущие расхождения цен, даже маленькие:")
        print("=" * 60)

        # get_settings() - синглтон (lru_cache), поэтому запоминаем
        # реальный порог ДО подмены, а не читаем settings после неё -
        # scanner._settings и settings - один и тот же объект.
        real_threshold = settings.NEGRISK_MIN_PROFIT_PCT

        scanner = NegRiskScanner()
        scanner._settings.NEGRISK_MIN_PROFIT_PCT = -1.0  # показать вообще все расклады
        all_found = []
        for market_id, legs in events.items():
            all_found.extend(scanner._scan_event(market_id, legs, books, fee_bips=0))
        scanner._settings.NEGRISK_MIN_PROFIT_PCT = real_threshold  # вернуть как было

        for o in all_found[:20]:
            print(
                f"  {o['kind']:15s} market={o['neg_risk_market_id'][:12]}… "
                f"profit={o['est_profit']:.3f} ({o['est_profit_pct']*100:+.2f}%)"
            )
        passing = sum(1 for o in all_found if o["est_profit_pct"] >= real_threshold)
        print(f"\nВсего раскладов (без фильтра по порогу): {len(all_found)}")
        print(f"Текущий порог NEGRISK_MIN_PROFIT_PCT = {real_threshold*100:.1f}% - из них прошли бы его: {passing}")


if __name__ == "__main__":
    asyncio.run(main())
