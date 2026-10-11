"""
On-chain клиент контракта NegRiskAdapter (Polygon).

Это контракт, который реализует шаги "Convert" и "Merge" стратегии: он
хранит учёт позиций для NegRisk (многоисходных) событий и позволяет
превращать токены No в пакет Yes, либо полный набор Yes+No ОДНОГО вопроса -
обратно в коллатерал.

Доки: https://docs.polymarket.com/advanced/neg-risk
Исходники: https://github.com/Polymarket/neg-risk-ctf-adapter

ВАЖНО - чего этот модуль НЕ делает:
  - Не хранит и не управляет приватным ключом. Подпись транзакций
    намеренно вынесена в отдельный метод sign_and_send с собственными
    предохранителями - см. ниже и services/execution.py.
  - Не размещает ордера на CLOB. Покупка No-позиций, которые потом
    конвертируются, идёт через CLOB API (py-clob-client-v2), не здесь.
  - Не делает разовую настройку, нужную каждому кошельку перед первым
    Convert/Merge: разрешение (`setApprovalForAll`) для NegRiskAdapter
    двигать ваши позиционные токены CTF (ERC-1155).
    `build_set_approval_tx` ниже строит эту транзакцию, но отправить её
    один раз - забота вызывающего кода.

ВАЖНО про сами формулы Convert/Merge - см. подробный разбор экономики
в negrisk_scanner.py; здесь только контрактные вызовы.

Адреса NEG_RISK_ADAPTER_ADDRESS / CTF_ADDRESS в config.py сверены по
https://docs.polymarket.com/resources/contracts - стоит свериться ещё раз
перед работой с реальными деньгами, адреса контрактов - ровно то, во что
не стоит верить на слово LLM без проверки.
"""

import logging
from typing import Any

from eth_account import Account
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from web3 import Web3
from web3.exceptions import Web3Exception

from app.config import get_settings

logger = logging.getLogger(__name__)


def _retry_rpc() -> Any:
    """Повторные попытки только для ЧТЕНИЯ с цепочки (view-вызовы) - узкий
    публичный RPC может моргать, и для чтения это безопасно повторить.
    Отправку подписанных транзакций (sign_and_send) НЕ оборачиваем этим
    декоратором - слепой повтор отправки транзакции может задвоить её."""
    return retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((Web3Exception, ConnectionError, TimeoutError)),
    )


# Минимальные фрагменты ABI - только те функции, что реально вызываются.
# Сверены побайтово с исходником верифицированного развёрнутого контракта
# на PolygonScan
# (https://polygonscan.com/address/0xd91e80cf2e7be2e162c6513ced06f1dd0da35296#code)
# 2026-09-27, и дополнительно проверено, что каждый селектор ниже реально
# присутствует в таблице диспетчеризации развёрнутого байткода - не
# просто упомянут в JSON ABI.
NEG_RISK_ADAPTER_ABI: list[dict[str, Any]] = [
    {
        "name": "convertPositions",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "_marketId", "type": "bytes32"},
            {"name": "_indexSet", "type": "uint256"},
            {"name": "_amount", "type": "uint256"},
        ],
        "outputs": [],
    },
    {
        "name": "mergePositions",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "_conditionId", "type": "bytes32"},
            {"name": "_amount", "type": "uint256"},
        ],
        "outputs": [],
    },
    {
        "name": "getFeeBips",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "_marketId", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "FEE_DENOMINATOR",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "getDetermined",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "_marketId", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "bool"}],
    },
]

# Часть стандартного ABI ERC-1155 - для разрешения доступа и проверки
# реального количества токенов на руках (после исполнения ордера на CLOB
# нужно узнать ТОЧНОЕ число полученных акций, прежде чем звать Convert -
# оценка по цене/сумме не годится, нужна правда с цепочки).
ERC1155_ABI: list[dict[str, Any]] = [
    {
        "name": "setApprovalForAll",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "outputs": [],
    },
    {
        "name": "isApprovedForAll",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

# Суммы токенов-исходов на Polymarket - с 6 знаками после запятой, как у
# USDC/pUSD.
TOKEN_DECIMALS = 6


def to_token_units(amount: float) -> int:
    return int(round(amount * (10**TOKEN_DECIMALS)))


def from_token_units(raw: int) -> float:
    return raw / (10**TOKEN_DECIMALS)


def index_set_for(outcome_indices: list[int]) -> int:
    """Собирает битовую маску `_indexSet` из позиций исходов (считая с 0)
    внутри списка исходов NegRisk-рынка."""
    mask = 0
    for i in outcome_indices:
        mask |= 1 << i
    return mask


class NegRiskAdapterClient:
    def __init__(self, w3: Web3 | None = None) -> None:
        settings = get_settings()
        self._settings = settings
        self.w3 = w3 or Web3(Web3.HTTPProvider(
            settings.POLYGON_RPC_URL,
            request_kwargs={"timeout": settings.POLYGON_RPC_TIMEOUT_SECONDS},
        ))
        self.adapter = self.w3.eth.contract(
            address=Web3.to_checksum_address(settings.NEG_RISK_ADAPTER_ADDRESS),
            abi=NEG_RISK_ADAPTER_ABI,
        )
        self.ctf = self.w3.eth.contract(
            address=Web3.to_checksum_address(settings.CTF_ADDRESS),
            abi=ERC1155_ABI,
        )

    # --- Чтение (безопасно, деньги не трогаются) ---

    @_retry_rpc()
    def get_fee_bips(self, market_id: str) -> int:
        return self.adapter.functions.getFeeBips(market_id).call()

    @_retry_rpc()
    def fee_denominator(self) -> int:
        return self.adapter.functions.FEE_DENOMINATOR().call()

    @_retry_rpc()
    def is_determined(self, market_id: str) -> bool:
        """True, если событие уже разрешилось. Никогда не делать
        Convert/Merge на разрешившемся рынке, рассчитывая на экономику
        арбитража - вместо этого нужен Redeem."""
        return self.adapter.functions.getDetermined(market_id).call()

    @_retry_rpc()
    def is_approved(self, owner: str) -> bool:
        return self.ctf.functions.isApprovedForAll(
            Web3.to_checksum_address(owner),
            Web3.to_checksum_address(self._settings.NEG_RISK_ADAPTER_ADDRESS),
        ).call()

    @_retry_rpc()
    def get_ctf_balance(self, owner: str, token_id: str) -> float:
        """Точное количество конкретного позиционного токена (Yes/No) на
        руках у `owner` - правда с цепочки. Используется, чтобы узнать
        РЕАЛЬНЫЙ размер заполнения ордера на покупку: сравниваем баланс
        до и после исполнения вместо того, чтобы оценивать количество
        акций по сумме в долларах и цене исполнения."""
        raw = self.ctf.functions.balanceOf(
            Web3.to_checksum_address(owner), int(token_id)
        ).call()
        return from_token_units(raw)

    # --- Сборщики неподписанных транзакций ---
    # Возвращают обычные словари. НЕ подписывают и не отправляют ничего -
    # см. sign_and_send, почему это отдельный, явно огороженный шаг.

    def build_set_approval_tx(self, owner: str) -> dict:
        """Разовая настройка: разрешает NegRiskAdapter двигать ваши
        позиционные токены CTF от вашего имени. Без этого Convert/Merge
        не сработают."""
        return self.ctf.functions.setApprovalForAll(
            Web3.to_checksum_address(self._settings.NEG_RISK_ADAPTER_ADDRESS), True
        ).build_transaction(self._base_tx(owner))

    def build_convert_tx(self, owner: str, market_id: str, index_set: int, amount: float) -> dict:
        return self.adapter.functions.convertPositions(
            market_id, index_set, to_token_units(amount)
        ).build_transaction(self._base_tx(owner))

    def build_merge_tx(self, owner: str, condition_id: str, amount: float) -> dict:
        return self.adapter.functions.mergePositions(
            condition_id, to_token_units(amount)
        ).build_transaction(self._base_tx(owner))

    @_retry_rpc()
    def _base_tx(self, owner: str) -> dict:
        owner = Web3.to_checksum_address(owner)
        return {
            "from": owner,
            "nonce": self.w3.eth.get_transaction_count(owner),
            "chainId": 137,  # Polygon mainnet
        }

    # --- Подпись и отправка: самая опасная часть ---

    def sign_and_send(self, tx: dict, private_key: str, *, confirm: bool) -> str:
        """Подписывает `tx` и отправляет в сеть. Тратит реальные
        деньги/газ.

        Намеренно требует confirm=True в каждом месте вызова - поверх
        settings.ENABLE_ONCHAIN_EXECUTION - два независимых переключателя,
        оба должны быть включены, прежде чем что-либо будет подписано, так
        что случайное значение по умолчанию или опечатка в конфиге не
        смогут отправить боевую транзакцию."""
        if not self._settings.ENABLE_ONCHAIN_EXECUTION:
            raise RuntimeError(
                "ENABLE_ONCHAIN_EXECUTION выключен - отказываюсь подписывать "
                "или отправлять. Это общий выключатель проекта; включайте в "
                ".env только после того, как проверили транзакцию, которую "
                "строит этот вызов."
            )
        if not confirm:
            raise RuntimeError("sign_and_send требует confirm=True в месте вызова.")

        signed = Account.sign_transaction(tx, private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        logger.warning("Отправлена транзакция %s", tx_hash.hex())
        return tx_hash.hex()
