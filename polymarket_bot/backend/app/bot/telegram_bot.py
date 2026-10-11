"""
Telegram-бот - единственный интерфейс управления ботом (по ТЗ). Показывает
статус фоновых процессов, живой баланс и найденные возможности, даёт
аварийный стоп для исполнителя. Сам по себе ничего не решает и не
торгует - только читает состояние других сервисов и дёргает их
безопасные ручки (pause/resume).

Доступ ограничен списком TELEGRAM_ADMIN_IDS из .env - единственная
граница безопасности между "кто угодно в Telegram" и управлением ботом,
который умеет тратить реальные деньги. Без заполненного списка бот
отвечает всем одинаково: "доступ запрещён".
"""

import logging

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message

from app.config import get_settings
from app.repository.database import async_session_factory
from app.repository.market_repo import get_recent_opportunities, get_total_realized_profit
from app.services.polymarket.wallet import get_current_bankroll_async

logger = logging.getLogger(__name__)

router = Router()


def _admin_ids(settings) -> set[int]:
    raw = settings.TELEGRAM_ADMIN_IDS.strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


def _is_admin(message: Message, settings) -> bool:
    admins = _admin_ids(settings)
    return bool(admins) and message.from_user is not None and message.from_user.id in admins


def _workers(message: Message) -> dict:
    return getattr(message.bot, "_polymarket_state", {}) or {}


@router.message(Command("start", "help"))
async def cmd_help(message: Message) -> None:
    settings = get_settings()
    if not _is_admin(message, settings):
        await message.answer(
            "Доступ запрещён. Ваш Telegram ID: "
            f"<code>{message.from_user.id if message.from_user else '?'}</code>\n"
            "Добавьте его в TELEGRAM_ADMIN_IDS в .env, чтобы получить доступ."
        )
        return
    await message.answer(
        "<b>polymarket_bot</b>\n\n"
        "/status — статус сборщика, сканера, исполнителя и текущий баланс\n"
        "/opportunities — последние найденные арбитражные возможности\n"
        "/pause — аварийный стоп: исполнитель перестаёт совершать сделки\n"
        "/resume — снять паузу\n"
    )


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    settings = get_settings()
    if not _is_admin(message, settings):
        return

    w = _workers(message)
    collector, scanner, executor = w.get("collector"), w.get("scanner"), w.get("executor")

    def fmt(worker, name: str) -> str:
        if worker is None:
            return f"<b>{name}</b>: не запущен в этом процессе"
        status = "ошибка" if getattr(worker, "last_cycle_error", None) else "ок"
        last = getattr(worker, "last_cycle_at", None)
        return f"<b>{name}</b>: {status}, последний цикл: {last}"

    lines = [fmt(collector, "Сборщик данных"), fmt(scanner, "Сканер NegRisk")]

    if executor is not None:
        paused = " (НА ПАУЗЕ)" if executor.is_paused else ""
        lines.append(fmt(executor, "Исполнитель") + paused)
    else:
        lines.append("<b>Исполнитель</b>: не запущен в этом процессе")
    lines.append(
        f"Исполнение реальных сделок: "
        f"{'включено' if settings.ENABLE_ONCHAIN_EXECUTION else 'выключено'}"
    )

    bankroll = await get_current_bankroll_async()
    if bankroll is not None:
        profit = 0.0
        async with async_session_factory() as session:
            profit = await get_total_realized_profit(session)
        lines.append(
            f"\n💰 Баланс pUSD: <b>${bankroll:.2f}</b> "
            f"(реализовано с начала: {'+' if profit >= 0 else ''}{profit:.2f})"
        )
    else:
        lines.append("\n💰 Баланс: не удалось прочитать (WALLET_ADDRESS не задан или RPC недоступен)")

    await message.answer("\n".join(lines))


@router.message(Command("opportunities"))
async def cmd_opportunities(message: Message) -> None:
    settings = get_settings()
    if not _is_admin(message, settings):
        return

    async with async_session_factory() as session:
        opps = await get_recent_opportunities(session, limit=10)

    if not opps:
        await message.answer("Возможностей пока не найдено.")
        return

    lines = ["<b>Последние найденные возможности:</b>"]
    for o in opps:
        lines.append(
            f"\n<b>{o.kind}</b> · {o.detected_at:%H:%M:%S}\n"
            f"market_id: <code>{o.neg_risk_market_id[:12]}…</code>\n"
            f"размер: {o.size:g} · прибыль: {o.est_profit:.2f} pUSD "
            f"({o.est_profit_pct * 100:.1f}%)"
        )
    await message.answer("\n".join(lines))


@router.message(Command("pause"))
async def cmd_pause(message: Message) -> None:
    settings = get_settings()
    if not _is_admin(message, settings):
        return
    executor = _workers(message).get("executor")
    if executor is None:
        await message.answer("Исполнитель не запущен в этом процессе - паузить нечего.")
        return
    executor.pause()
    await message.answer("⏸ Исполнитель поставлен на паузу. Сделки совершаться не будут.")


@router.message(Command("resume"))
async def cmd_resume(message: Message) -> None:
    settings = get_settings()
    if not _is_admin(message, settings):
        return
    executor = _workers(message).get("executor")
    if executor is None:
        await message.answer("Исполнитель не запущен в этом процессе.")
        return
    executor.resume()
    await message.answer("▶️ Пауза снята.")


@router.message(F.text)
async def fallback(message: Message) -> None:
    settings = get_settings()
    if not _is_admin(message, settings):
        return
    await message.answer("Неизвестная команда. /help — список команд.")


async def notify_admins(bot: Bot, settings, text: str) -> None:
    for admin_id in _admin_ids(settings):
        try:
            await bot.send_message(admin_id, text)
        except Exception:  # noqa: BLE001 - один недоступный админ не должен рвать рассылку
            logger.exception("Не удалось отправить уведомление админу %s", admin_id)


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher()
    dp.include_router(router)
    return dp


def build_bot(settings) -> Bot:
    return Bot(token=settings.TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
