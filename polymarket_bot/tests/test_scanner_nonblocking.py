import asyncio, sys, time
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), "..", "backend"))
from app.repository.database import init_db, async_session_factory
from app.repository.market_repo import upsert_market, save_order_book_snapshot
from app.services.strategy.negrisk_scanner import NegRiskScanner
from app.services.polymarket.negrisk_adapter import NegRiskAdapterClient

calls = []
def slow_fee(self, market_id):
    calls.append(market_id)
    time.sleep(0.3)          # имитация медленного синхронного RPC (блокирующий вызов)
    return 0
NegRiskAdapterClient.get_fee_bips = slow_fee

def book(ask, bid):
    return {'bids': [{'price': str(bid), 'size': '1000'}], 'asks': [{'price': str(ask), 'size': '1000'}]}

async def seed(n_events=700, n_opp_events=30):
    await init_db()
    async with async_session_factory() as s:
        for e in range(n_events):
            has_opp = e < n_opp_events
            for i in range(3):
                cid, yt, nt = f'e{e}c{i}', f'e{e}y{i}', f'e{e}n{i}'
                await upsert_market(s, {'condition_id': cid, 'question': f'Q{e}-{i}', 'active': True, 'closed': False,
                    'clob_token_ids': [yt, nt], 'neg_risk': True, 'neg_risk_market_id': f'0xev{e}',
                    'neg_risk_augmented': False, 'neg_risk_outcome_index': i})
                no_ask = 0.20 if (has_opp and i == 0) else 0.68   # дешёвый No -> convert_single
                for tok, (ask, bid) in ((yt, (0.34, 0.32)), (nt, (no_ask, no_ask - 0.02))):
                    await save_order_book_snapshot(s, {'token_id': tok, 'condition_id': cid, 'raw_book': book(ask, bid)})
        await s.commit()

async def main():
    await seed()
    sc = NegRiskScanner()
    sc._settings.NEGRISK_SCAN_SIZE = 10.0
    sc._settings.NEGRISK_MIN_PROFIT_PCT = 0.01
    sc._settings.NEGRISK_MAX_FEE_LOOKUPS_PER_CYCLE = 20

    gaps, last = [], time.monotonic()
    stop = False
    async def heartbeat():            # "FastAPI": должен тикать, пока сканер работает
        nonlocal last
        while not stop:
            await asyncio.sleep(0.05)
            now = time.monotonic(); gaps.append(now - last); last = now
    hb = asyncio.create_task(heartbeat())

    t0 = time.monotonic()
    found = await sc.run_once()
    elapsed = time.monotonic() - t0
    stop = True; await hb

    print(f'событий всего: 700, с кандидатами: 30')
    print(f'run_once: {elapsed:.2f}с, найдено возможностей: {found}')
    print(f'запросов комиссии в цепочку: {len(calls)} (лимит 20; раньше было бы 700)')
    print(f'максимальная пауза event loop: {max(gaps)*1000:.0f} мс')
    assert len(calls) == 20, calls
    assert max(gaps) < 0.5, 'event loop блокировался!'
    # второй цикл: кэш + оставшиеся события
    calls.clear()
    await sc.run_once()
    print(f'2-й цикл: новых запросов комиссии {len(calls)} (кэш работает, добираем остаток)')
    assert len(calls) == 10
    print('OK: event loop не блокируется, RPC дёргается только по кандидатам и в пределах лимита')

asyncio.run(main())
