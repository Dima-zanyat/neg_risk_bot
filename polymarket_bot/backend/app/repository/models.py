from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Market(Base):
    """Нормализованные метаданные рынка из Gamma API.
    Одна строка на рынок, обновляется (upsert) каждый цикл сбора."""

    __tablename__ = "markets"

    condition_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    question: Mapped[str] = mapped_column(Text)
    slug: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category: Mapped[str | None] = mapped_column(String(120), nullable=True)

    active: Mapped[bool] = mapped_column(Boolean, default=False)
    closed: Mapped[bool] = mapped_column(Boolean, default=False)

    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Хранится как JSON: outcomes ["Yes","No"], outcome_prices ["0.65","0.35"],
    # clob_token_ids ["<yes_token_id>","<no_token_id>"] - индексы согласованы.
    outcomes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    outcome_prices: Mapped[list | None] = mapped_column(JSON, nullable=True)
    clob_token_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)

    end_date: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # Поля NegRisk (многоисходные события). neg_risk_market_id группирует
    # рынки, относящиеся к одному событию, для целей Convert/Merge - именно
    # это значение передаётся как `_marketId` в контракт NegRiskAdapter.
    neg_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    neg_risk_market_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    neg_risk_augmented: Mapped[bool] = mapped_column(Boolean, default=False)
    # Битовый индекс этого исхода внутри NegRisk-рынка (поле Gamma
    # groupItemThreshold) - обязателен для построения _indexSet в
    # convertPositions. См. комментарий в gamma_client.py.
    neg_risk_outcome_index: Mapped[int | None] = mapped_column(Integer, nullable=True)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class OrderBookSnapshot(Base):
    """Срез стакана заявок на момент времени для одного токена-исхода.
    Намеренно без обновления (append-only) - это временной ряд, по
    которому потом выбирают и считают, по каждому токену отдельно."""

    __tablename__ = "order_book_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token_id: Mapped[str] = mapped_column(String(120), index=True)
    condition_id: Mapped[str] = mapped_column(String(80), index=True)

    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )

    best_bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_bid_size: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask_size: Mapped[float | None] = mapped_column(Float, nullable=True)
    midpoint: Mapped[float | None] = mapped_column(Float, nullable=True)

    tick_size: Mapped[float | None] = mapped_column(Float, nullable=True)
    neg_risk: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Сырой ответ целиком, как есть - чтобы ничего не терялось, даже если
    # разбор полей выше несовершенен или формат API изменится.
    raw_book: Mapped[dict] = mapped_column(JSON)


class NegRiskOpportunity(Base):
    """Найденная (не обязательно исполненная) арбитражная возможность в
    NegRisk-событии - для обзора/бэктеста и для алертов в Telegram.
    Append-only - сканер никогда не обновляет строку, просто пишет новую
    каждый цикл, если возможность всё ещё существует."""

    __tablename__ = "negrisk_opportunities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )

    neg_risk_market_id: Mapped[str] = mapped_column(String(80), index=True)
    # "convert_all" (купить No по всем исходам, конвертировать разом -
    # pUSD без остатка) или "convert_single" (купить No по одному исходу,
    # получить пакет Yes по остальным - нужно ещё продать).
    kind: Mapped[str] = mapped_column(String(32))
    # Для convert_single: по какому исходу куплен No. Пусто для convert_all.
    outcome_condition_id: Mapped[str | None] = mapped_column(String(80), nullable=True)

    size: Mapped[float] = mapped_column(Float)
    est_cost: Mapped[float] = mapped_column(Float)
    est_value: Mapped[float] = mapped_column(Float)
    est_fee: Mapped[float] = mapped_column(Float)
    est_profit: Mapped[float] = mapped_column(Float)
    est_profit_pct: Mapped[float] = mapped_column(Float)

    # Всё, на чём основан расчёт (цены по каждому исходу, доступная
    # глубина книги и т.п.) - для отладки и бэктеста.
    details: Mapped[dict] = mapped_column(JSON)


class ExecutionState(Base):
    """Журнал исполнения одной возможности - для восстановления после
    падения/перезапуска. Каждая попытка исполнения проходит через фазы
    (см. PHASE_* ниже), и после КАЖДОЙ фазы запись обновляется ПЕРЕД тем,
    как начать следующую - так что если процесс упадёт посреди
    последовательности "купили No, ещё не сконвертировали", при старте
    это будет видно по незавершённой записи, а не потеряется молча.

    Executor при старте обязан проверить таблицу на незавершённые записи
    (status не в {"done","failed"}) и сообщить о них в Telegram, а не
    продолжать как ни в чём не бывало - автоматическое доигрывание
    прерванной последовательности рискованнее, чем ручное решение
    человека, что с ней делать."""

    __tablename__ = "execution_states"

    PHASE_BUYING_NO = "buying_no"
    PHASE_BOUGHT_NO = "bought_no"
    PHASE_CONVERTING = "converting"
    PHASE_CONVERTED = "converted"
    PHASE_SELLING = "selling"  # только для convert_single
    PHASE_DONE = "done"
    PHASE_FAILED = "failed"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[int] = mapped_column(Integer, index=True)
    kind: Mapped[str] = mapped_column(String(32))
    neg_risk_market_id: Mapped[str] = mapped_column(String(80))

    phase: Mapped[str] = mapped_column(String(32), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    # Сколько реально потрачено/получено на каждом шаге - заполняется по
    # мере прохождения фаз, а не оценивается заранее.
    spent_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    no_filled_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    convert_tx_hash: Mapped[str | None] = mapped_column(String(100), nullable=True)
    sell_proceeds_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Всё остальное, что может пригодиться при разборе случая вручную
    # (token_id'ы, index_set и т.п.).
    details: Mapped[dict] = mapped_column(JSON, default=dict)


class ExecutionLedger(Base):
    """Реализованный результат каждой завершённой сделки - основа для
    отображения "сколько заработано всего" в Telegram и для расчёта
    текущего капитала, если on-chain баланс недоступен. Append-only."""

    __tablename__ = "execution_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    execution_state_id: Mapped[int] = mapped_column(Integer, index=True)
    kind: Mapped[str] = mapped_column(String(32))
    spent_usd: Mapped[float] = mapped_column(Float)
    received_usd: Mapped[float] = mapped_column(Float)
    realized_profit_usd: Mapped[float] = mapped_column(Float)
