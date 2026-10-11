import asyncio, sys, time
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), "..", "backend"))
from app.services.strategy.negrisk_scanner import NegRiskScanner
from app.services.polymarket.negrisk_adapter import NegRiskAdapterClient

NegRiskAdapterClient.get_fee_bips = lambda self, m: time.sleep(4) or 0   # "зависший" RPC

async def main():
    sc = NegRiskScanner()
    sc._settings.POLYGON_RPC_TIMEOUT_SECONDS = 1      # итоговый таймаут = 2с
    sc._settings.NEGRISK_FEE_BIPS_FALLBACK = 50
    ticks = []
    async def hb():
        while True:
            await asyncio.sleep(0.05); ticks.append(time.monotonic())
    t = asyncio.create_task(hb())
    t0 = time.monotonic()
    fee = await sc._fee_bips('0xabc')
    print(f'fee={fee} (ожидаем запасное 50) за {time.monotonic()-t0:.1f}с; кэш: {sc._fee_bips_cache}')
    assert fee == 50 and time.monotonic() - t0 < 3.5
    gaps = [b - a for a, b in zip(ticks, ticks[1:])]
    print(f'макс. пауза event loop: {max(gaps)*1000:.0f} мс')
    assert max(gaps) < 0.3
    t.cancel()
    print('OK: зависший RPC не блокирует loop, работает запасное значение')
asyncio.run(main())
