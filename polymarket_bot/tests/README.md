Регрессионные проверки "event loop не блокируется" (запуск из корня проекта):

    python tests/test_scanner_nonblocking.py   # 700 событий + медленный RPC
    python tests/test_rpc_hang.py              # полностью зависший RPC -> таймаут и запасная комиссия

Правило проекта: web3 синхронный. Любой вызов цепочки из async-кода обязан идти
через asyncio.to_thread + таймаут (см. wallet.get_current_bankroll_async и
NegRiskScanner._fee_bips), иначе замирает весь процесс, включая FastAPI.
