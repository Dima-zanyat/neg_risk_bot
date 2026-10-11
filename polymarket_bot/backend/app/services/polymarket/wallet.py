"""
Чтение баланса pUSD кошелька on-chain. На этом основано накопление
капитала: Executor считает размер каждой сделки долей от РЕАЛЬНОГО
текущего баланса (см. EXECUTION_TRADE_FRACTION в config.py), а не от
фиксированной стартовой суммы - поэтому прибыль автоматически увеличивает
следующие сделки, а убыток уменьшает, без ручной перенастройки.

pUSD - обычный ERC-20 (адрес подтверждён по
https://docs.polymarket.com/resources/contracts), поэтому здесь используется
только стандартный вызов balanceOf - ничего специфичного для Polymarket.
"""

import asyncio
import logging

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from web3 import Web3
from web3.exceptions import Web3Exception

from app.config import get_settings

logger = logging.getLogger(__name__)

ERC20_BALANCE_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "decimals",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint8"}],
    },
]


def _retry_rpc():
    return retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((Web3Exception, ConnectionError, TimeoutError)),
    )


class WalletBalance:
    def __init__(self, w3: Web3 | None = None) -> None:
        settings = get_settings()
        self._settings = settings
        self.w3 = w3 or Web3(Web3.HTTPProvider(
            settings.POLYGON_RPC_URL,
            request_kwargs={"timeout": settings.POLYGON_RPC_TIMEOUT_SECONDS},
        ))
        self._pusd = self.w3.eth.contract(
            address=Web3.to_checksum_address(settings.PUSD_ADDRESS),
            abi=ERC20_BALANCE_ABI,
        )
        self._decimals_cache: int | None = None

    @_retry_rpc()
    def decimals(self) -> int:
        if self._decimals_cache is None:
            self._decimals_cache = self._pusd.functions.decimals().call()
        return self._decimals_cache

    @_retry_rpc()
    def get_pusd_balance(self, owner: str) -> float:
        """Текущий баланс pUSD владельца `owner`, в долларах (с учётом
        decimals токена - читается с контракта, не захардкожен)."""
        raw = self._pusd.functions.balanceOf(Web3.to_checksum_address(owner)).call()
        return raw / (10 ** self.decimals())


def get_current_bankroll(wallet: WalletBalance | None = None) -> float | None:
    """Возвращает текущий баланс pUSD адреса из настроек, либо None, если
    адрес не задан или баланс прочитать не удалось (например, RPC
    недоступен). None - намеренно не 0 и не STARTING_BANKROLL_USD: вызывающий
    код (Executor) обязан воспринимать None как "не знаю баланс" и
    пропускать цикл исполнения, а не тихо подставлять угаданное число."""
    settings = get_settings()
    if not settings.WALLET_ADDRESS:
        return None
    try:
        w = wallet or WalletBalance()
        return w.get_pusd_balance(settings.WALLET_ADDRESS)
    except Exception:  # noqa: BLE001 - сбой RPC не должен ронять вызывающий код
        logger.warning("Не удалось прочитать баланс pUSD с цепочки", exc_info=True)
        return None


async def get_current_bankroll_async(timeout: float = 20.0) -> float | None:
    """Асинхронная обёртка: web3 синхронный, и прямой вызов
    get_current_bankroll() из async-кода (health-эндпоинт, Telegram,
    исполнитель) блокирует ВЕСЬ event loop на время RPC-запроса - при
    медленном публичном RPC это замораживает и FastAPI тоже. Здесь запрос
    уходит в поток с общим таймаутом; при таймауте возвращается None
    (= "баланс неизвестен", как и при любой другой ошибке чтения)."""
    try:
        return await asyncio.wait_for(asyncio.to_thread(get_current_bankroll), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("Чтение баланса pUSD не уложилось в %.0fс", timeout)
        return None
