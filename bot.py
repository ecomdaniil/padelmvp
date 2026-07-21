"""Telegram-бот записи на игры и тренировки (aiogram).

Регистрация, расписание, бронирование, оплата (заглушка), напоминания,
связь с администратором. Запуск: ``python bot.py`` или фоном из ``app.py``.
"""

import asyncio
import hashlib
import html
import logging
import os
import re
import secrets
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional

from dotenv import load_dotenv
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    CallbackQuery,
    TelegramObject,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from apscheduler.schedulers.background import BackgroundScheduler

import cache
import database_async as db
import payment_provider
from bot_content import (
    BTN_ABOUT_PADEL,
    BTN_COACHES,
    BTN_CONTACT_ADMIN,
    BTN_GAMES,
    BTN_MAIN_MENU,
    BTN_MY_BOOKINGS,
    BTN_PAST_GAMES,
    BTN_STATS,
    MENU_BUTTONS,
    PADEL_INFO_TEXT,
)

load_dotenv()

# ADMIN_CHAT_ID из env (можно несколько через запятую). Дополнительно —
# club_info.admin_telegram_id из CRM / команды /bindadmin.
_ADMIN_CHAT_IDS_ENV = [
    part.strip()
    for part in (os.getenv("ADMIN_CHAT_ID") or "").split(",")
    if part.strip()
]
# Обратная совместимость: старый код и проверки читают «первый» id.
ADMIN_CHAT_ID = _ADMIN_CHAT_IDS_ENV[0] if _ADMIN_CHAT_IDS_ENV else None
_ADMIN_BIND_TOKEN_RAW = (os.getenv("ADMIN_BIND_TOKEN") or "").strip()
ADMIN_BIND_TOKEN = _ADMIN_BIND_TOKEN_RAW if len(_ADMIN_BIND_TOKEN_RAW) >= 24 else ""
ADMIN_BIND_ALLOW_REBIND = os.getenv("ADMIN_BIND_ALLOW_REBIND", "0").lower() in {
    "1", "true", "yes",
}
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
_FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY") or ""
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN") or (
    hashlib.sha256(f"tg-webhook:{_FLASK_SECRET_KEY}".encode("utf-8")).hexdigest()
    if _FLASK_SECRET_KEY
    else ""
)
# Простые антибрут-лимиты для /bindadmin (in-process).
_bindadmin_fails: dict[int, list[float]] = {}
_BINDADMIN_MAX_FAILS = 5
_BINDADMIN_FAIL_WINDOW_SEC = 900.0

# По умолчанию логируем только ошибки — это отдельно настраиваемо через .env,
# если для отладки понадобится более подробный вывод (INFO/DEBUG).
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.ERROR))
logger = logging.getLogger(__name__)

if _ADMIN_BIND_TOKEN_RAW and not ADMIN_BIND_TOKEN:
    logger.error("ADMIN_BIND_TOKEN короче 24 символов — /bindadmin отключён")

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Ключи/TTL кэша живут в cache.py — там же их читает app.py (CRM), чтобы
# сбрасывать кэш игр при создании/редактировании игры или заявки. Раньше
# CRM ничего не знала об этом кэше, из-за чего бот мог до 30 сек (или до
# перезапуска процесса, если бот и CRM — разные процессы без Redis)
# показывать устаревший список игр после изменений в CRM.
GAMES_CACHE_KEY = cache.GAMES_CACHE_KEY
GAMES_CACHE_TTL = cache.GAMES_CACHE_TTL
LEVELS_CACHE_KEY = cache.LEVELS_CACHE_KEY
LEVELS_CACHE_TTL = cache.LEVELS_CACHE_TTL
USER_CACHE_PREFIX = cache.USER_CACHE_PREFIX
USER_CACHE_TTL = cache.USER_CACHE_TTL
_USER_NONE = "__none__"  # sentinel в кэше: пользователя нет (не путать с miss)

RATE_LIMIT_PER_SECOND = int(os.getenv("BOT_RATE_LIMIT_PER_SECOND", "10"))

# Команды бота (меню "/" в Telegram-клиенте) — регистрируются через
# setMyCommands при старте (см. setup_bot_commands(), вызывается и из
# main() здесь, и из run_bot() в app.py, если бот встроен в CRM-процесс).
BOT_COMMANDS = [
    BotCommand(command="start", description="Начать / открыть анкету"),
    BotCommand(command="menu", description="Главное меню"),
    BotCommand(command="myprofile", description="Мой профиль"),
    BotCommand(command="help", description="Написать администратору"),
]


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(BOT_COMMANDS)


class ThrottlingMiddleware(BaseMiddleware):
    """Простой rate limiting: не более RATE_LIMIT_PER_SECOND событий
    (сообщений/нажатий кнопок) от одного пользователя за скользящее окно
    в 1 секунду. Защищает бота и БД от флуда/спама одним пользователем.

    """

    def __init__(self, limit: int = 10, window_seconds: float = 1.0):
        self.limit = limit
        self.window_seconds = window_seconds
        self._events: Dict[int, deque] = defaultdict(deque)

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        user_id = user.id if user else None

        if user_id is not None:
            now = time.monotonic()
            events = self._events[user_id]
            while events and now - events[0] > self.window_seconds:
                events.popleft()

            if len(events) >= self.limit:
                logger.debug("Rate limit exceeded for user %s", user_id)
                # Снимаем «часики» Telegram на callback — иначе UX кажется зависанием.
                if isinstance(event, CallbackQuery):
                    try:
                        await event.answer("Слишком быстро, подожди секунду")
                    except Exception:
                        pass
                return None

            events.append(now)

        return await handler(event, data)


router = Router()
router.message.middleware(ThrottlingMiddleware(limit=RATE_LIMIT_PER_SECOND))
router.callback_query.middleware(ThrottlingMiddleware(limit=RATE_LIMIT_PER_SECOND))

VALID_LEVELS = {"Новичок", "Любитель", "Продвинутый", "Профессионал"}
YES_ANSWERS = {"да", "yes"}
NO_ANSWERS = {"нет", "no"}


async def get_valid_levels() -> set:
    """Уровни игроков сейчас статичны, но кладём их в кэш — если позже
    справочник уровней переедет в БД (CRM), достаточно поменять loader здесь,
    и весь остальной код бота не изменится."""
    cached = cache.get(LEVELS_CACHE_KEY)
    if cached is not None:
        return set(cached)
    cache.set(LEVELS_CACHE_KEY, list(VALID_LEVELS), LEVELS_CACHE_TTL)
    return VALID_LEVELS

PADEL_RULES_TEXT = (
    "📖 <b>Правила игры в падел</b>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🎾 <b>Основы</b>\n"
    "• Падел играется на закрытом корте со стеклянными стенами.\n"
    "• Формат: парный (2 на 2).\n"
    "• Мяч подаётся снизу, удар выполняется ниже пояса.\n"
    "• После подачи мяч должен отскочить от пола на стороне соперника.\n\n"
    "🏟 <b>Использование стен</b>\n"
    "• После отскока от пола мяч можно отбить от стен своей стороны.\n"
    "• Мяч можно отбить от стен соперника, если он сначала коснулся пола на их стороне.\n"
    "• Мяч, попавший в сетку и перелетевший на сторону соперника, считается в игре.\n\n"
    "📊 <b>Счёт</b>\n"
    "• Используется система как в теннисе: 15 → 30 → 40 → гейм.\n"
    "• При счёте 40:40 — преимущество, затем победа в гейме.\n"
    "• Матч обычно до 2 сетов (6 геймов в сете, тай-брейк при 6:6).\n\n"
    "⚠️ <b>Ошибки</b>\n"
    "• Мяч касается стены до отскока от пола.\n"
    "• Мяч попадает в сетку и не перелетает на сторону соперника.\n"
    "• Мяч вылетает за пределы корта (через дверь или верх).\n"
    "• Двойное касание ракеткой.\n\n"
    "🤝 <b>Этикет</b>\n"
    "• Уважайте партнёров и соперников.\n"
    "• Не мешайте соседним кортам.\n"
    "• Соблюдайте очерёдность подачи.\n\n"
    "Приятной игры! 🎾"
)


# ---------------------------------------------------------------------------
# FSM-состояния для заполнения анкеты
# ---------------------------------------------------------------------------

class RegistrationForm(StatesGroup):
    waiting_for_name = State()
    waiting_for_age = State()
    waiting_for_level = State()
    waiting_for_inventory = State()
    waiting_for_rules = State()
    waiting_for_phone = State()


class AdminContact(StatesGroup):
    waiting_for_message = State()


class AdminReply(StatesGroup):
    """FSM-состояние для чата администратора: используется, когда админ
    нажимает «↩️ Ответить» на переданное игроком сообщение (/help)."""
    waiting_for_reply = State()


MAX_SLOTS_PER_BOOKING = 4


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Постоянная клавиатура главного меню."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_GAMES), KeyboardButton(text=BTN_MY_BOOKINGS)],
            [KeyboardButton(text=BTN_PAST_GAMES), KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_COACHES), KeyboardButton(text=BTN_ABOUT_PADEL)],
            [KeyboardButton(text=BTN_CONTACT_ADMIN)],
            [KeyboardButton(text=BTN_MAIN_MENU)],
        ],
        resize_keyboard=True,
    )


EXPERIENCE_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Новичок")],
        [KeyboardButton(text="Любитель")],
        [KeyboardButton(text="Продвинутый")],
        [KeyboardButton(text="Профессионал")],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)

YES_NO_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Да"), KeyboardButton(text="Нет")],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)


def _format_profile(user: dict) -> str:
    """Форматирует профиль пользователя для отображения."""
    inventory = "Да" if user.get("has_inventory") else "Нет" if user.get("has_inventory") is not None else "—"
    rules = "Да" if user.get("needs_rules") else "Нет" if user.get("needs_rules") is not None else "—"

    return (
        f"👤 <b>Ваш профиль</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Имя: {_html(user['name'])}\n"
        f"Возраст: {_html(user.get('age') or '—')}\n"
        f"Опыт: {_html(user['level'])}\n"
        f"Свой инвентарь: {inventory}\n"
        f"Нужны правила: {rules}\n"
        f"Телефон: {_html(user['phone'])}\n"
    )


def _safe_text(message: Message) -> str:
    """Возвращает текст сообщения без исключения для пустых/не-текстовых обновлений."""
    return (message.text or "").strip()


def _html(value) -> str:
    """Экранирует пользовательский/БД-текст перед parse_mode=HTML в Telegram."""
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


_admin_ids_cache: Optional[tuple[float, List[int]]] = None
_ADMIN_IDS_CACHE_TTL = 60.0


async def _resolve_admin_chat_ids() -> List[int]:
    """Список chat_id админов: env ADMIN_CHAT_ID + club_info.admin_telegram_id."""
    global _admin_ids_cache
    now = time.monotonic()
    if _admin_ids_cache and now - _admin_ids_cache[0] < _ADMIN_IDS_CACHE_TTL:
        return list(_admin_ids_cache[1])

    ids: List[int] = []
    for raw in _ADMIN_CHAT_IDS_ENV:
        try:
            ids.append(int(raw))
        except ValueError:
            logger.error("ADMIN_CHAT_ID содержит нечисловое значение: %r", raw)
    try:
        db_id = await db.get_club_admin_telegram_id()
        if db_id is not None:
            ids.append(int(db_id))
    except Exception as e:
        logger.error("Не удалось прочитать admin_telegram_id из БД: %s", e)
    # уникальные, порядок сохраняем
    seen = set()
    unique = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            unique.append(i)
    _admin_ids_cache = (now, unique)
    return list(unique)


async def _is_admin_user(user_id: Optional[int]) -> bool:
    """Только личный telegram id админа может отвечать игрокам от имени клуба."""
    if user_id is None:
        return False
    admin_ids = await _resolve_admin_chat_ids()
    return int(user_id) in admin_ids


async def _send_admin_message(bot: Bot, text: str, **kwargs) -> Optional[Any]:
    """Шлёт сообщение всем известным админам. Возвращает первое успешное Message."""
    admin_ids = await _resolve_admin_chat_ids()
    if not admin_ids:
        logger.warning("Нет ADMIN_CHAT_ID / club_info.admin_telegram_id — сообщение админу пропущено")
        return None
    first_ok = None
    last_error = None
    for chat_id in admin_ids:
        try:
            sent = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            if first_ok is None:
                first_ok = sent
        except Exception as e:
            last_error = e
            logger.error("Не удалось отправить сообщение админу %s: %s", chat_id, e)
    if first_ok is None and last_error is not None:
        raise last_error
    return first_ok


def _profile_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редактировать анкету", callback_data="edit_profile")],
        [InlineKeyboardButton(text="🎾 Посмотреть игры", callback_data="show_games")],
    ])


async def show_main_menu(message: Message, user: dict):
    """Показывает главное меню зарегистрированному пользователю."""
    await message.answer(
        f"🏠 <b>Главное меню</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выберите раздел ниже для быстрого доступа:\n"
        "• 🎾 Игры — посмотреть и записаться\n"
        "• 📋 Мои записи — предстоящие заявки\n"
        "• 🏆 Сыгранные игры — история прошедших игр\n"
        "• 📊 Моя статистика — оценить активность\n"
        "• 💬 Связаться с администратором — задать вопрос",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


async def _start_questionnaire(message: Message, state: FSMContext, is_edit: bool = False):
    """Запускает анкету с первого вопроса."""
    await state.update_data(is_edit=is_edit)
    await state.set_state(RegistrationForm.waiting_for_name)

    greeting = "✏️ Давай обновим твою анкету!" if is_edit else (
        "🎾 Привет! Это бот для записи на игры в падел.\n\n"
        "Для начала давай заполним твою анкету — это займёт пару минут."
    )
    await message.answer(
        f"{greeting}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 1 из 6</b>\n\n"
        "👤 Как тебя зовут?\n\n"
        "<i>Напиши имя как обычно: например, Александр</i>",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML",
    )


async def _save_profile(message: Message, state: FSMContext):
    """Сохраняет анкету (создание или обновление) и показывает итог."""
    data = await state.get_data()
    is_edit = data.get("is_edit", False)

    if is_edit:
        await db.update_user(
            telegram_id=message.from_user.id,
            name=data["name"],
            phone=data["phone"],
            level=data["level"],
            age=data.get("age"),
            city=None,
            has_inventory=data.get("has_inventory"),
            needs_rules=data.get("needs_rules"),
        )
        title = "Анкета обновлена! ✅"
    else:
        await db.create_user(
            telegram_id=message.from_user.id,
            name=data["name"],
            phone=data["phone"],
            level=data["level"],
            age=data.get("age"),
            city=None,
            has_inventory=data.get("has_inventory"),
            needs_rules=data.get("needs_rules"),
        )
        title = "Спасибо, анкета заполнена! ✅"

    _invalidate_user_cache(message.from_user.id)
    await state.clear()

    inventory_text = "Да ✅" if data.get("has_inventory") else "Нет"
    rules_text = "Да" if data.get("needs_rules") else "Нет"

    await message.answer(
        f"{title}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Имя: {data['name']}\n"
        f"Возраст: {data.get('age')}\n"
        f"Опыт: {data['level']}\n"
        f"Инвентарь: {inventory_text}\n"
        f"Правила объяснены: {rules_text}\n"
        f"Телефон: {data['phone']}\n\n"
        "✅ Теперь ты можешь открыть раздел «🎾 Игры» и записаться на ближайший корт.",
        reply_markup=ReplyKeyboardRemove(),
    )

    user = await db.get_user_by_telegram_id(message.from_user.id)
    if user:
        await show_main_menu(message, user)


# ---------------------------------------------------------------------------
# /start и заполнение анкеты
# ---------------------------------------------------------------------------

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    existing_user = await db.get_user_by_telegram_id(message.from_user.id)

    if existing_user:
        await message.answer(
            f"С возвращением, {existing_user['name']}! 🎾\n\n"
            + _format_profile(existing_user),
            reply_markup=_profile_keyboard(),
            parse_mode="HTML",
        )
        await show_main_menu(message, existing_user)
        return

    await _start_questionnaire(message, state, is_edit=False)


async def _intercept_fsm_commands(message: Message, state: FSMContext) -> bool:
    """FSM-хендлеры перехватывают /cancel раньше cmd_cancel — обрабатываем здесь.
    True = команда обработана, вызывающий handler должен выйти."""
    text = (message.text or "").strip()
    if not text.startswith("/"):
        return False
    cmd = text.split()[0].split("@")[0].lower()
    await state.clear()
    if cmd == "/cancel":
        user = await _require_registered(message.from_user.id)
        if user:
            await message.answer(
                "Действие отменено.",
                reply_markup=main_menu_keyboard(),
            )
            await show_main_menu(message, user)
        else:
            await message.answer(
                "Действие отменено. Отправь /start, чтобы начать заново."
            )
        return True
    if cmd == "/start":
        await cmd_start(message, state)
        return True
    if cmd == "/menu":
        user = await _require_registered(message.from_user.id)
        if user:
            await show_main_menu(message, user)
        else:
            await message.answer("Сначала заполни анкету: отправь /start")
        return True
    await message.answer(
        "Действие сброшено. Используй кнопки меню или /start."
    )
    return True


@router.message(Command("myprofile"))
async def cmd_myprofile(message: Message):
    """Команда из мини-меню Telegram («/») — показать анкету игрока."""
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await message.answer(
        _format_profile(user),
        reply_markup=_profile_keyboard(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "edit_profile")
async def edit_profile(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await _start_questionnaire(callback.message, state, is_edit=True)


@router.callback_query(F.data == "show_games")
async def show_games_from_profile(callback: CallbackQuery):
    await callback.answer()
    # callback.message.from_user — это бот (сообщение от бота), а не игрок.
    # Профиль проверяем по callback.from_user.id.
    await _ask_game_format(callback.message, telegram_id=callback.from_user.id)


@router.callback_query(F.data.startswith("games_format:"))
async def process_games_format(callback: CallbackQuery):
    """Сингл / Классика / Тренировки."""
    kind = (callback.data.split(":")[1] if ":" in callback.data else "").strip()
    await callback.answer()
    if kind == "training":
        await _show_trainings(callback.message, telegram_id=callback.from_user.id)
        return
    try:
        total_slots = int(kind)
    except ValueError:
        await callback.message.answer("Некорректный выбор формата.")
        return
    if total_slots not in (2, 4):
        await callback.message.answer("Некорректный выбор формата.")
        return
    await _show_games(
        callback.message,
        total_slots=total_slots,
        telegram_id=callback.from_user.id,
    )


@router.message(StateFilter(RegistrationForm.waiting_for_name), F.text)
async def process_name(message: Message, state: FSMContext):
    if await _intercept_fsm_commands(message, state):
        return
    name = _safe_text(message)
    if name in MENU_BUTTONS:
        await message.answer(
            "❌ Это не похоже на имя.\n"
            "Введи настоящее имя текстом (минимум 2 символа):"
        )
        return
    if len(name) < 2:
        await message.answer(
            "❌ Имя слишком короткое.\n"
            "Пожалуйста, введи настоящее имя (минимум 2 символа):"
        )
        return

    await state.update_data(name=name)
    await state.set_state(RegistrationForm.waiting_for_age)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 2 из 6</b>\n\n"
        "🎂 Сколько тебе лет?\n\n"
        "<i>Введи число, например: 28</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_age), F.text)
async def process_age(message: Message, state: FSMContext):
    if await _intercept_fsm_commands(message, state):
        return
    text = _safe_text(message)
    if not text.isdigit():
        await message.answer(
            "❌ Пожалуйста, введи возраст цифрами.\n"
            "Например: <b>25</b>",
            parse_mode="HTML",
        )
        return

    age = int(text)
    if age < 5 or age > 99:
        await message.answer("❌ Укажи реальный возраст (от 5 до 99 лет):")
        return

    await state.update_data(age=age)
    await state.set_state(RegistrationForm.waiting_for_level)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 3 из 6</b>\n\n"
        "🎾 Какой у тебя опыт игры в падел?\n\n"
        "<i>Выбери вариант из кнопок ниже:</i>",
        reply_markup=EXPERIENCE_KEYBOARD,
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_level), F.text)
async def process_level(message: Message, state: FSMContext):
    if await _intercept_fsm_commands(message, state):
        return
    level = _safe_text(message)
    valid_levels = await get_valid_levels()
    if level not in valid_levels:
        await message.answer(
            "❌ Пожалуйста, выбери уровень с помощью кнопок ниже:",
            reply_markup=EXPERIENCE_KEYBOARD,
        )
        return

    await state.update_data(level=level)
    await state.set_state(RegistrationForm.waiting_for_inventory)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 4 из 6</b>\n\n"
        "🎒 Есть ли у тебя свой инвентарь (ракетка, мячи)?",
        reply_markup=YES_NO_KEYBOARD,
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_inventory), F.text)
async def process_inventory(message: Message, state: FSMContext):
    if await _intercept_fsm_commands(message, state):
        return
    answer = _safe_text(message).lower()
    if answer not in YES_ANSWERS | NO_ANSWERS:
        await message.answer(
            "❌ Пожалуйста, выбери ответ кнопкой — Да или Нет:",
            reply_markup=YES_NO_KEYBOARD,
        )
        return

    has_inventory = answer in YES_ANSWERS
    await state.update_data(has_inventory=has_inventory)
    await state.set_state(RegistrationForm.waiting_for_rules)

    if not has_inventory:
        await message.answer(
            "🎒 Инвентарь выдаётся бесплатно прямо в клубе.\n"
            "Приходи — всё предоставим! ✅"
        )

    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 5 из 6</b>\n\n"
        "📖 Нужно ли объяснить правила игры в падел?",
        reply_markup=YES_NO_KEYBOARD,
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_rules), F.text)
async def process_rules(message: Message, state: FSMContext):
    if await _intercept_fsm_commands(message, state):
        return
    answer = _safe_text(message).lower()
    if answer not in YES_ANSWERS | NO_ANSWERS:
        await message.answer(
            "❌ Пожалуйста, выбери ответ кнопкой — Да или Нет:",
            reply_markup=YES_NO_KEYBOARD,
        )
        return

    needs_rules = answer in YES_ANSWERS
    await state.update_data(needs_rules=needs_rules)

    if needs_rules:
        await message.answer(PADEL_RULES_TEXT, parse_mode="HTML")

    await state.set_state(RegistrationForm.waiting_for_phone)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 6 из 6 (последний!)</b>\n\n"
        "📞 Оставь свой номер телефона.\n\n"
        "<i>Введи только цифры, например: 79001234567</i>",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_phone), F.text)
async def process_phone(message: Message, state: FSMContext):
    if await _intercept_fsm_commands(message, state):
        return
    # Принимаем только цифры (без букв, пробелов и символов)
    phone = re.sub(r"\D", "", _safe_text(message))

    if not phone or not phone.isdigit():
        await message.answer(
            "❌ Номер должен содержать <b>только цифры</b>.\n"
            "Попробуй ещё раз, например: <b>79001234567</b>",
            parse_mode="HTML",
        )
        return

    if len(phone) < 10 or len(phone) > 15:
        await message.answer(
            "❌ Номер слишком короткий или длинный.\n"
            "Введи от 10 до 15 цифр:",
        )
        return

    await state.update_data(phone=phone)
    await _save_profile(message, state)


# ---------------------------------------------------------------------------
# Главное меню и навигация
# ---------------------------------------------------------------------------

async def _get_upcoming_games_cached() -> list:
    """Список ближайших игр с количеством занятых мест — одним агрегирующим
    запросом (без N+1) и с коротким TTL-кэшем, чтобы не бить в БД при каждом
    открытии раздела «Игры». Кэш инвалидируется сразу после записи/отмены
    (в боте) и после создания/редактирования игры или смены статуса заявки
    (в CRM, см. app.py) — обе стороны используют один и тот же ключ/backend
    из cache.py."""
    cached = cache.get(GAMES_CACHE_KEY)
    if cached is not None:
        logger.debug("Список игр отдан из кэша (%s)", cache.backend_name())
        return cached
    t0 = time.monotonic()
    games = await db.get_upcoming_games_with_slots()
    logger.debug("Запрос списка игр к БД занял %.3f с", time.monotonic() - t0)
    cache.set(GAMES_CACHE_KEY, games, GAMES_CACHE_TTL)
    return games


def _invalidate_games_cache() -> None:
    cache.invalidate_games_cache()


async def _send_answer(message: Message, text: str, keyboard: Optional[InlineKeyboardMarkup]) -> None:
    """Обёртка вокруг message.answer(), приведённая к настоящей корутине.

    message.answer() в установленной версии aiogram — синхронный метод,
    который сразу возвращает объект запроса SendMessage (он awaitable через
    __await__, но это не корутина). asyncio.gather() у таких объектов не
    работает: они на основе pydantic и не хэшируемы, из-за чего gather()
    падал с `TypeError: unhashable type: 'SendMessage'` ДО того, как успевал
    отправить хоть один запрос — то есть все карточки после заголовочного
    сообщения молча пропадали. Оборачиваем в async def, чтобы gather() имел
    дело с обычными корутинами."""
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


def _game_format_keyboard() -> InlineKeyboardMarkup:
    """Выбор: Сингл / Классика / Тренировки."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сингл (2 игрока)", callback_data="games_format:2")],
        [InlineKeyboardButton(text="Классика (4 игрока)", callback_data="games_format:4")],
        [InlineKeyboardButton(text="Тренировки", callback_data="games_format:training")],
    ])


async def _ask_game_format(message: Message, telegram_id: Optional[int] = None) -> None:
    """Первый шаг раздела «Игры»: спрашиваем формат или тренировки.

    telegram_id — id игрока в Telegram. Нужен явно при вызове из callback:
    у callback.message.from_user стоит бот, а не человек, нажавший кнопку."""
    user = await _require_registered(telegram_id or message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await message.answer(
        "🎾 <b>Что вы ищете?</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери формат игры или тренировку с тренером:",
        reply_markup=_game_format_keyboard(),
        parse_mode="HTML",
    )


def _game_card(game: dict) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    taken = game["taken"]
    free_slots = game["total_slots"] - taken
    is_training = (game.get("event_type") or "game") == "training"

    lines = []
    if is_training and game.get("title"):
        lines.append(f"💪 <b>{_html(game['title'])}</b>")
    lines.append(
        f"📅 <b>{game['game_date'].strftime('%d.%m.%Y')}</b> в {str(game['game_time'])[:5]}"
    )
    if is_training and game.get("coach_name"):
        emoji = game.get("coach_emoji") or "🧑‍🏫"
        lines.append(f"{emoji} Тренер: {_html(game['coach_name'])}")
    lines.append(f"📍 {_html(game['location'])}")
    lines.append(f"💰 {_html(game['price'])} ₽")
    lines.append(f"👥 Свободно мест: <b>{free_slots}</b> из {game['total_slots']}")
    text = "\n".join(lines)

    if free_slots > 0:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Записаться", callback_data=f"book:{game['id']}")]
        ])
    else:
        text += "\n\n❌ <b>Мест нет</b>"
        keyboard = None

    return text, keyboard


async def _show_games(
    message: Message,
    total_slots: int,
    telegram_id: Optional[int] = None,
):
    """Показывает ближайшие обычные игры выбранного формата (2 или 4)."""
    t_start = time.monotonic()
    user = await _require_registered(telegram_id or message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    format_label = "Сингл (2 игрока)" if total_slots == 2 else "Классика (4 игрока)"
    games = [
        g for g in await _get_upcoming_games_cached()
        if (g.get("event_type") or "game") == "game"
        and int(g.get("total_slots") or 0) == total_slots
    ]
    if not games:
        await message.answer(
            f"😔 Пока нет доступных игр формата «{format_label}».\n\n"
            "Загляни позже — мы добавляем новые игры регулярно!",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer(
        f"🎾 <b>Ближайшие игры — {format_label}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери игру и нажми «Записаться»:",
        parse_mode="HTML",
    )

    for game in games:
        text, keyboard = _game_card(game)
        try:
            await _send_answer(message, text, keyboard)
        except Exception as e:
            logger.error("Не удалось отправить карточку игры #%s: %s", game.get("id"), e)

    logger.debug(
        "_show_games: всего %.3f с, %d игр (формат %s)",
        time.monotonic() - t_start, len(games), total_slots,
    )


async def _show_trainings(message: Message, telegram_id: Optional[int] = None):
    """Ближайшие тренировки с тренером."""
    user = await _require_registered(telegram_id or message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    games = [
        g for g in await _get_upcoming_games_cached()
        if (g.get("event_type") or "game") == "training"
    ]
    if not games:
        await message.answer(
            "😔 Пока нет доступных тренировок.\n\n"
            "Загляни позже — мы добавляем новые занятия регулярно!",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer(
        "<b>Ближайшие тренировки</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери занятие и нажми «Записаться»:",
        parse_mode="HTML",
    )
    for game in games:
        text, keyboard = _game_card(game)
        try:
            await _send_answer(message, text, keyboard)
        except Exception as e:
            logger.error("Не удалось отправить карточку тренировки #%s: %s", game.get("id"), e)


async def _show_my_bookings(message: Message):
    """Показывает предстоящие (ещё не начавшиеся) записи пользователя."""
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    bookings = await db.get_active_bookings_for_user(user["id"])
    if not bookings:
        await message.answer(
            "📋 У тебя нет предстоящих записей.\n\n"
            "Посмотри доступные игры в «🎾 Игры» или историю в «🏆 Сыгранные игры».",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer(
        "📋 <b>Твои записи</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Только предстоящие игры. Можно докупить места, пока набор не полный, "
        "или отменить запись:",
        parse_mode="HTML",
    )

    def _booking_card(b: dict) -> tuple[str, InlineKeyboardMarkup]:
        status_emoji = "✅" if b['status'] == 'подтверждена' else "⏳"
        free_slots = int(b.get("free_slots") or 0)
        total_slots = int(b.get("total_slots") or 0)
        text = (
            f"{_event_card_header(b)}\n"
            f"📅 <b>{b['game_date'].strftime('%d.%m.%Y')}</b> в {str(b['game_time'])[:5]}\n"
            f"📍 {_html(b['location'])}\n"
            f"👥 Твоих мест: {b.get('slots_count', 1)}\n"
        )
        if total_slots:
            taken = max(0, total_slots - free_slots)
            text += f"📊 Набор: {taken}/{total_slots}\n"
        text += f"📌 Статус: {status_emoji} {_html(b['status'])}"
        rows = []
        if free_slots > 0:
            rows.append([InlineKeyboardButton(
                text="➕ Докупить места",
                callback_data=f"buy_more:{b['id']}",
            )])
        rows.append([InlineKeyboardButton(
            text="❌ Отменить запись",
            callback_data=f"cancel_ask:{b['id']}",
        )])
        keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
        return text, keyboard

    sends = [_send_answer(message, *_booking_card(b)) for b in bookings]
    results = await asyncio.gather(*sends, return_exceptions=True)
    for b, result in zip(bookings, results):
        if isinstance(result, Exception):
            logger.error("Не удалось отправить карточку записи #%s: %s", b.get("id"), result)


async def _show_past_bookings(message: Message):
    """История сыгранных / уже прошедших игр."""
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    bookings = await db.get_past_bookings_for_user(user["id"])
    if not bookings:
        await message.answer(
            "🏆 Пока нет сыгранных игр.\n\n"
            "После первой прошедшей игры она появится здесь.",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer(
        "🏆 <b>Сыгранные игры</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "История прошедших записей:",
        parse_mode="HTML",
    )

    def _past_card(b: dict) -> tuple[str, Optional[InlineKeyboardMarkup]]:
        # Список уже отфильтрован как CRM «Посещения» — это прошедшие записи.
        if b.get("no_show"):
            status_line = "❌ Не пришёл"
        else:
            status_line = "✅ Посещение"
        text = (
            f"{_event_card_header(b)}\n"
            f"📅 <b>{b['game_date'].strftime('%d.%m.%Y')}</b> в {str(b['game_time'])[:5]}\n"
            f"📍 {_html(b['location'])}\n"
            f"👥 Мест: {b.get('slots_count', 1)}\n"
            f"{status_line}"
        )
        return text, None

    sends = [_send_answer(message, *_past_card(b)) for b in bookings]
    results = await asyncio.gather(*sends, return_exceptions=True)
    for b, result in zip(bookings, results):
        if isinstance(result, Exception):
            logger.error("Не удалось отправить карточку сыгранной игры #%s: %s", b.get("id"), result)


def _event_card_header(event: dict) -> str:
    """Шапка карточки в «Мои записи» / «Сыгранные»: Тренировка: … или Игра: Сингл/Классика."""
    is_training = (event.get("event_type") or "game") == "training"
    if is_training:
        title = (event.get("title") or "").strip()
        if title:
            return f"<b>Тренировка: {_html(title)}</b>"
        return "<b>Тренировка</b>"
    slots = int(event.get("total_slots") or 0)
    if slots == 2:
        return "<b>Игра: Сингл</b>"
    if slots == 4:
        return "<b>Игра: Классика</b>"
    return "<b>Игра</b>"


def _format_statistics(stats: dict) -> str:
    return (
        "📊 <b>Моя статистика</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📝 Всего заявок подано: <b>{stats['total']}</b>\n"
        f"💳 Оплачено: <b>{stats['paid']}</b>\n"
        f"✅ Посещений: <b>{stats['attended']}</b>\n"
        f"🚫 Неявок: <b>{stats.get('no_shows', 0)}</b>\n"
        f"❌ Отменено: <b>{stats['cancelled']}</b>\n\n"
        f"📈 Посещаемость: <b>{stats['attendance_rate']}%</b>\n"
        f"⏱ Сыграно часов: <b>{stats['hours_played']}</b>\n\n"
        "Так держать 👏"
    )


def _coaches_list_keyboard(coaches: list) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            text=f"{c.get('emoji') or '🧑‍🏫'} {c['name']}",
            callback_data=f"coach:{c['id']}",
        )]
        for c in coaches
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(F.text == BTN_MAIN_MENU)
@router.message(Command("menu"))
async def menu_main(message: Message, state: FSMContext):
    await state.clear()
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer("Сначала заполни анкету: отправь /start")
        return
    await show_main_menu(message, user)


@router.message(F.text == BTN_GAMES)
async def menu_games(message: Message, state: FSMContext):
    await state.clear()
    await _ask_game_format(message)


@router.message(F.text == BTN_MY_BOOKINGS)
async def menu_my_bookings(message: Message, state: FSMContext):
    await state.clear()
    await _show_my_bookings(message)


@router.message(F.text == BTN_PAST_GAMES)
async def menu_past_games(message: Message, state: FSMContext):
    await state.clear()
    await _show_past_bookings(message)


def _format_club_about_text(info: Optional[dict]) -> str:
    """Текст «О клубе» из CRM с учётом галочек bot_show_*."""
    if not info:
        return PADEL_INFO_TEXT
    lines = ["ℹ️ <b>О клубе</b>", "━━━━━━━━━━━━━━━━━━━━", ""]
    shown = False
    if info.get("bot_show_name") and (info.get("name") or "").strip():
        lines.append(f"🏟 <b>{_html(info['name'])}</b>")
        shown = True
    if info.get("bot_show_description") and (info.get("description") or "").strip():
        lines.append(_html(info["description"]))
        shown = True
    if info.get("bot_show_city") and (info.get("city") or "").strip():
        lines.append(f"🏙 Город: {_html(info['city'])}")
        shown = True
    if info.get("bot_show_address") and (info.get("address") or "").strip():
        lines.append(f"📍 Адрес: {_html(info['address'])}")
        shown = True
    if info.get("bot_show_phone") and (info.get("contact_phone") or "").strip():
        lines.append(f"📞 Телефон: {_html(info['contact_phone'])}")
        shown = True
    if info.get("bot_show_email") and (info.get("contact_email") or "").strip():
        lines.append(f"✉️ Email: {_html(info['contact_email'])}")
        shown = True
    if info.get("bot_show_admin_username") and (info.get("admin_telegram_username") or "").strip():
        uname = str(info["admin_telegram_username"]).lstrip("@")
        lines.append(f"👤 Администратор: @{_html(uname)}")
        shown = True
    if not shown:
        return PADEL_INFO_TEXT
    return "\n".join(lines)


@router.message(F.text == BTN_ABOUT_PADEL)
async def menu_about_padel(message: Message, state: FSMContext):
    await state.clear()
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    info = await db.get_club_info()
    await message.answer(
        _format_club_about_text(info),
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(F.text == BTN_STATS)
async def menu_stats(message: Message, state: FSMContext):
    await state.clear()
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    stats = await db.get_user_statistics(user["id"])
    await message.answer(
        _format_statistics(stats),
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(F.text == BTN_COACHES)
async def menu_coaches(message: Message, state: FSMContext):
    await state.clear()
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    coaches = await db.get_active_coaches()
    if not coaches:
        await message.answer(
            "👨‍🏫 Пока нет активных тренеров.\n"
            "Загляни позже!",
            reply_markup=main_menu_keyboard(),
        )
        return
    await message.answer(
        "👨‍🏫 <b>Наши тренеры</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери тренера, чтобы узнать подробнее:",
        reply_markup=_coaches_list_keyboard(coaches),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("coach:"))
async def show_coach_detail(callback: CallbackQuery):
    coach_id = int(callback.data.split(":")[1])
    coach = await db.get_coach_by_id(coach_id)
    if not coach or not coach.get("is_active"):
        await callback.answer("Тренер не найден", show_alert=True)
        return

    await callback.answer()
    emoji = coach.get("emoji") or "🧑‍🏫"
    lines = [
        f"{emoji} <b>{_html(coach['name'])}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]
    if coach.get("specialization"):
        lines.append(f"📝 {_html(coach['specialization'])}")
        lines.append("")
    if coach.get("experience"):
        lines.append(f"⏱ Стаж: {_html(coach['experience'])}")
    if coach.get("phone"):
        lines.append(f"📞 {_html(coach['phone'])}")
    if coach.get("telegram_username"):
        lines.append(f"✈️ @{_html(coach['telegram_username'])}")
    if coach.get("achievements"):
        lines.append("")
        lines.append(f"🏆 <b>Достижения:</b>\n{_html(coach['achievements'])}")
    lines.append("")
    lines.append("<i>Запись на тренировку — в разделе «🎾 Игры» → «Тренировки».</i>")
    await callback.message.answer(
        "\n".join(lines),
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(Command("bindadmin"))
async def cmd_bindadmin(message: Message):
    """Привязка Telegram ID админа: /bindadmin <токен из ADMIN_BIND_TOKEN>.
    Нужно, если на Render не задан ADMIN_CHAT_ID и связь с админом «недоступна».

    Защиты: длинный токен (≥24), rate-limit ошибок, по умолчанию нельзя
    перезаписать уже привязанного админа (ADMIN_BIND_ALLOW_REBIND=1)."""
    parts = (message.text or "").split(maxsplit=1)
    token = parts[1].strip() if len(parts) > 1 else ""
    tg_id = int(message.from_user.id) if message.from_user else 0
    if not ADMIN_BIND_TOKEN:
        await message.answer(
            "Привязка через бота выключена. Укажи ADMIN_CHAT_ID в env "
            "или Telegram ID в CRM → «О клубе»."
        )
        return

    now = time.monotonic()
    fails = [t for t in _bindadmin_fails.get(tg_id, []) if now - t < _BINDADMIN_FAIL_WINDOW_SEC]
    _bindadmin_fails[tg_id] = fails
    if len(fails) >= _BINDADMIN_MAX_FAILS:
        await message.answer("Слишком много попыток. Попробуй позже.")
        return

    token_ok = (
        bool(token)
        and len(token) == len(ADMIN_BIND_TOKEN)
        and secrets.compare_digest(token, ADMIN_BIND_TOKEN)
    )
    if not token_ok:
        _bindadmin_fails.setdefault(tg_id, []).append(now)
        await message.answer("❌ Неверный токен.")
        return

    if not ADMIN_BIND_ALLOW_REBIND:
        existing = await db.get_club_admin_telegram_id()
        if existing and int(existing) != tg_id:
            await message.answer(
                "Администратор уже привязан. Смени ADMIN_CHAT_ID в env "
                "или задай ADMIN_BIND_ALLOW_REBIND=1 для перепривязки."
            )
            return

    try:
        await db.set_club_admin_telegram_id(tg_id)
    except Exception as e:
        logger.error("bindadmin failed: %s", e)
        await message.answer("❌ Не удалось сохранить. Попробуй позже.")
        return
    _bindadmin_fails.pop(tg_id, None)
    global _admin_ids_cache
    _admin_ids_cache = None
    await message.answer(
        f"✅ Готово. Твой Telegram ID <code>{tg_id}</code> "
        "сохранён как администратор бота.\n"
        "Теперь «Связаться с администратором» будет работать.",
        parse_mode="HTML",
    )


@router.message(F.text == BTN_CONTACT_ADMIN)
@router.message(Command("help"))
async def menu_contact_admin(message: Message, state: FSMContext):
    await state.clear()
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    await state.set_state(AdminContact.waiting_for_message)
    await message.answer(
        "💬 <b>Связь с администратором</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Напиши своё сообщение — оно будет передано администратору.\n\n"
        f"<i>Для отмены нажми «{BTN_MAIN_MENU}»</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(AdminContact.waiting_for_message), F.text)
async def process_admin_message(message: Message, state: FSMContext, bot: Bot):
    if await _intercept_fsm_commands(message, state):
        return
    # Кнопки меню не должны уходить админу как текст сообщения.
    if message.text in MENU_BUTTONS:
        await state.clear()
        user = await _require_registered(message.from_user.id)
        if user:
            await show_main_menu(message, user)
        return

    user = await _require_registered(message.from_user.id)
    if not user:
        await state.clear()
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    if not message.text or not message.text.strip():
        await message.answer("❌ Отправь текстовое сообщение:")
        return

    user_message = message.text.strip()
    await state.clear()

    admin_ids = await _resolve_admin_chat_ids()
    if not admin_ids:
        await message.answer(
            "⚠️ Связь с администратором временно недоступна.\n\n"
            "Администратору нужно указать свой Telegram ID в CRM "
            "(«О клубе») или в переменной ADMIN_CHAT_ID на сервере.",
            reply_markup=main_menu_keyboard(),
        )
        return

    admin_text = (
        "💬 <b>Сообщение от игрока (/help)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 {_html(user['name'])}\n"
        f"📞 {_html(user['phone'])}\n"
        f"🆔 Telegram ID: {message.from_user.id}\n\n"
        f"📝 {_html(user_message)}"
    )
    # Кнопка «Ответить» — нажатие запускает FSM-диалог AdminReply прямо
    # в чате администратора (см. admin_reply_start/admin_reply_send).
    admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Ответить", callback_data=f"reply_to:{message.from_user.id}")]
    ])
    try:
        await _send_admin_message(
            bot, admin_text, parse_mode="HTML", reply_markup=admin_keyboard
        )
        await message.answer(
            "✅ Сообщение отправлено администратору!\n\n"
            "Ответ придёт в этот чат.",
            reply_markup=main_menu_keyboard(),
        )
    except Exception as e:
        logger.error("Не удалось отправить сообщение админу: %s", e)
        await message.answer(
            "❌ Не удалось отправить сообщение.\n"
            "Попробуй позже.",
            reply_markup=main_menu_keyboard(),
        )


@router.callback_query(F.data.startswith("reply_to:"))
async def admin_reply_start(callback: CallbackQuery, state: FSMContext):
    """Админ нажал «↩️ Ответить» — только telegram id из списка админов."""
    # Сначала снимаем «часики», потом проверки/FSM — иначе UI кажется зависшим.
    await callback.answer()
    if not await _is_admin_user(callback.from_user.id):
        await callback.message.answer("❌ Недостаточно прав.")
        return
    target_telegram_id = int(callback.data.split(":", 1)[1])
    await state.update_data(reply_target_telegram_id=target_telegram_id)
    await state.set_state(AdminReply.waiting_for_reply)
    await callback.message.answer(
        "✏️ Напиши ответ игроку — я перешлю его в бот.\n"
        "<i>Для отмены отправь /cancel</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(AdminReply.waiting_for_reply), F.text)
async def admin_reply_send(message: Message, state: FSMContext, bot: Bot):
    if await _intercept_fsm_commands(message, state):
        return
    if not await _is_admin_user(message.from_user.id):
        await state.clear()
        await message.answer("❌ Недостаточно прав.")
        return

    data = await state.get_data()
    target_telegram_id = data.get("reply_target_telegram_id")
    await state.clear()

    reply_text = _safe_text(message)
    if not target_telegram_id or not reply_text:
        await message.answer("❌ Ответ не отправлен: пустое сообщение.")
        return

    try:
        await bot.send_message(
            target_telegram_id,
            f"💬 <b>Ответ администратора:</b>\n\n{_html(reply_text)}",
            parse_mode="HTML",
        )
        await message.answer("✅ Ответ отправлен игроку.")
    except Exception as e:
        logger.error("Не удалось отправить ответ игроку %s: %s", target_telegram_id, e)
        await message.answer(
            "❌ Не удалось отправить ответ — возможно, игрок заблокировал бота."
        )


# ---------------------------------------------------------------------------
# Список игр и запись
# ---------------------------------------------------------------------------

def _invalidate_user_cache(telegram_id: int) -> None:
    cache.delete(f"{USER_CACHE_PREFIX}{telegram_id}")


async def _require_registered(telegram_id: int):
    """Возвращает пользователя или None. Кэш на USER_CACHE_TTL — иначе
    каждый клик по меню ReplyKeyboard ждёт ~1с round-trip к Neon только
    ради проверки анкеты."""
    key = f"{USER_CACHE_PREFIX}{telegram_id}"
    cached = cache.get(key)
    if cached is not None:
        return None if cached == _USER_NONE else cached
    user = await db.get_user_by_telegram_id(telegram_id)
    cache.set(key, _USER_NONE if user is None else user, USER_CACHE_TTL)
    return user


async def _notify_admin_new_booking(
    bot: Bot, user: dict, game: dict, booking_id: int, slots_count: int, total_price: float,
):
    """Отправляет админу уведомление о новой записи игрока на корт и
    сохраняет message_id в bookings.admin_notify_message_id — при отмене
    до оплаты это сообщение удаляется (см. _delete_admin_booking_notify)."""
    game_datetime = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
    )
    is_training = (game.get("event_type") or "game") == "training"
    title_line = ""
    if is_training and game.get("title"):
        title_line = f"Тренировка: <b>{_html(game['title'])}</b>\n"
    elif is_training:
        title_line = "Тренировка\n"
    notification_text = (
        "🔔 <b>Новая запись на корт!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{title_line}"
        f"👤 Имя: {_html(user['name'])}\n"
        f"📞 Телефон: {_html(user['phone'])}\n"
        f"📅 Дата и время: {_html(game_datetime)}\n"
        f"📍 Корт / площадка: {_html(game['location'])}\n"
        f"👥 Мест: {slots_count}\n"
        f"💰 К оплате: {total_price:.0f} ₽\n"
        f"🆔 ID заявки: {booking_id}"
    )
    try:
        sent = await _send_admin_message(bot, notification_text, parse_mode="HTML")
        if sent is None:
            return
        try:
            await db.set_booking_admin_notify_message(booking_id, sent.message_id)
        except Exception as e:
            logger.error(
                "Не удалось сохранить message_id уведомления для брони #%s: %s",
                booking_id, e,
            )
    except Exception as e:
        logger.error("Не удалось отправить уведомление админу о записи #%s: %s", booking_id, e)


async def _delete_admin_booking_notify(bot: Bot, message_id: Optional[int]) -> None:
    """Удаляет у админа сообщение о записи/докупке, если заказ отменён до оплаты.
    Пробуем все admin chat_id — message_id мог уйти не в первый из списка."""
    if not message_id:
        return
    for chat_id in await _resolve_admin_chat_ids():
        try:
            await bot.delete_message(chat_id=chat_id, message_id=int(message_id))
        except Exception as e:
            logger.debug("Не удалось удалить уведомление админу %s msg=%s: %s", chat_id, message_id, e)


async def _cleanup_admin_notifies_after_cancel(bot: Bot, result: dict) -> None:
    """После отмены покупки/докупки убрать соответствующие сообщения у админа."""
    if result.get("status") == "extra_cancelled":
        # Снята только доплата — исходное «Новая запись» оставляем.
        await _delete_admin_booking_notify(bot, result.get("admin_extra_notify_message_id"))
        return
    # Полная отмена до оплаты (или с удалением pending) — чистим оба уведомления.
    if result.get("payment_deleted") or not result.get("had_payment"):
        await _delete_admin_booking_notify(bot, result.get("admin_notify_message_id"))
        await _delete_admin_booking_notify(bot, result.get("admin_extra_notify_message_id"))


@router.message(Command("games"))
async def cmd_games(message: Message):
    await _ask_game_format(message)


def _slots_choice_keyboard(game_id: int, max_slots: int) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(text=str(n), callback_data=f"book_slots:{game_id}:{n}")
        for n in range(1, max_slots + 1)
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        buttons,
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="book_back")],
    ])


def _buy_more_slots_keyboard(booking_id: int, max_slots: int) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(text=str(n), callback_data=f"buy_more_slots:{booking_id}:{n}")
        for n in range(1, max_slots + 1)
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        buttons,
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="buy_more_back")],
    ])


def _last_hour_fill_keyboard(game_id: int, free_slots: int) -> InlineKeyboardMarkup:
    """Меньше часа до старта — только выкуп всех оставшихся мест."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"✅ Выкупить все {free_slots} мест",
            callback_data=f"book_slots:{game_id}:{free_slots}",
        )],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="book_back")],
    ])


def _buy_more_last_hour_keyboard(booking_id: int, free_slots: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"✅ Выкупить все {free_slots} мест",
            callback_data=f"buy_more_slots:{booking_id}:{free_slots}",
        )],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="buy_more_back")],
    ])


def _must_fill_all_text(free_slots: int, total_slots: int) -> str:
    return (
        "⏰ <b>До начала игры меньше часа</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Частичную запись уже нельзя: игра скоро начнётся, и недобор "
        f"состава (<b>{total_slots - free_slots}/{total_slots}</b> → нужно "
        f"<b>{total_slots}/{total_slots}</b>) приведёт к отмене.\n\n"
        f"Можно выкупить <b>все оставшиеся места ({free_slots})</b> "
        "или не записываться."
    )


async def _prompt_slots_for_game(message: Message, game_id: int) -> bool:
    """Показывает вопрос «Сколько мест?» для игры. True — сообщение отправлено,
    False — игра недоступна / мест нет (ошибка уже отправлена вызывающему
    через return False — вызывающий сам решает, как ответить callback)."""
    offer = await db.get_game_slot_offer(game_id)
    if not offer:
        return False

    game = offer["game"]
    free_slots = offer["free_slots"]
    total_slots = offer["total_slots"]

    # Меньше часа до старта — только полный выкуп оставшихся мест.
    if offer["within_last_hour"]:
        await message.answer(
            _must_fill_all_text(free_slots, total_slots) +
            f"\n\n💰 Цена за место: {game['price']} ₽\n"
            f"Итого за {free_slots}: <b>{float(game['price']) * free_slots:.0f} ₽</b>",
            reply_markup=_last_hour_fill_keyboard(game_id, free_slots),
            parse_mode="HTML",
        )
        return True

    max_choice = min(MAX_SLOTS_PER_BOOKING, free_slots)
    await message.answer(
        "👥 <b>Сколько мест забронировать?</b>\n"
        f"Свободно: {free_slots} из {total_slots}\n"
        f"Цена за место: {game['price']} ₽\n\n"
        "Выбери количество:",
        reply_markup=_slots_choice_keyboard(game_id, max_choice),
        parse_mode="HTML",
    )
    return True


@router.callback_query(F.data.regexp(r"^book:\d+$"))
async def process_booking_ask_slots(callback: CallbackQuery):
    """Первый шаг записи: показываем игру и спрашиваем, на сколько мест
    бронировать (1-4), прежде чем создавать заявку.

    Фильтр именно ^book:\\d+$, а не startswith('book:') — иначе сюда же
    попадали бы book_slots:... и book_back."""
    # Снимаем «часики» Telegram сразу — проверка анкеты/мест может занять
    # round-trip к Neon; ошибки покажем обычным сообщением.
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    game_id = int(callback.data.split(":")[1])
    ok = await _prompt_slots_for_game(callback.message, game_id)
    if not ok:
        await callback.message.answer(
            "Эта игра временно недоступна: другой игрок уже заявил оплату, "
            "и администратор ещё подтверждает её.\n"
            "Или мест уже нет — загляни в «🎾 Игры» чуть позже."
        )


@router.callback_query(F.data == "book_back")
async def process_book_back(callback: CallbackQuery):
    """Назад с выбора количества мест — снова спрашиваем формат игры."""
    await callback.answer()
    await _ask_game_format(callback.message, telegram_id=callback.from_user.id)


def _fire_and_forget(coro, name: str) -> None:
    """Фоновая задача: ошибки только в лог, не роняют обработчик игрока."""
    task = asyncio.create_task(coro)

    def _done(t: asyncio.Task) -> None:
        try:
            exc = t.exception()
        except asyncio.CancelledError:
            return
        if exc:
            logger.error("Фоновая задача %s упала: %s", name, exc, exc_info=exc)

    task.add_done_callback(_done)


def _format_game_when(game: dict) -> str:
    """Дата/время игры безопасно для текста (date/time/str из asyncpg)."""
    gd = game.get("game_date")
    gt = game.get("game_time")
    if hasattr(gd, "strftime"):
        date_s = gd.strftime("%d.%m.%Y")
    else:
        date_s = str(gd or "")[:10]
    time_s = str(gt or "")[:5]
    return f"{date_s} в {time_s}".strip()


@router.callback_query(F.data.startswith("book_slots:"))
async def process_booking_confirm(callback: CallbackQuery):
    """Второй шаг записи: пользователь выбрал количество мест — создаём
    заявку, считаем итоговую цену и предлагаем способ оплаты."""
    # Мгновенный toast вместо крутящихся часиков на время create_booking_safe.
    try:
        await callback.answer("Записываю…")
    except Exception:
        pass

    try:
        await _do_booking_confirm(callback)
    except asyncio.TimeoutError:
        logger.error("Таймаут записи book_slots data=%s", callback.data)
        await callback.message.answer(
            "❌ База данных отвечает слишком долго.\n"
            "Попробуй ещё раз через несколько секунд.",
            reply_markup=main_menu_keyboard(),
        )
    except Exception as e:
        logger.exception("Ошибка записи book_slots data=%s: %s", callback.data, e)
        await callback.message.answer(
            "❌ Не удалось записаться из‑за временной ошибки.\n"
            "Попробуй ещё раз или открой «📋 Мои записи».",
            reply_markup=main_menu_keyboard(),
        )


async def _do_booking_confirm(callback: CallbackQuery) -> None:
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.message.answer("Некорректные данные записи.")
        return
    try:
        game_id = int(parts[1])
        slots_count = int(parts[2])
    except ValueError:
        await callback.message.answer("Некорректные данные записи.")
        return
    if slots_count < 1:
        await callback.message.answer("Некорректное количество мест.")
        return

    # Проверка мест и вставка заявки — атомарно; таймаут чтобы Neon/лок
    # не оставляли игрока с одним toast «Записываю…» без ответа.
    result = await asyncio.wait_for(
        db.create_booking_safe(
            user_id=user["id"], game_id=game_id, slots_count=slots_count,
        ),
        timeout=15,
    )

    if result["status"] == "not_found":
        await callback.message.answer("Эта игра больше не доступна.")
        return
    if result["status"] == "payment_hold":
        await callback.message.answer(
            "Эта игра временно недоступна для записи.\n"
            "Другой игрок уже заявил оплату — дождитесь подтверждения администратором. "
            "После этого свободные места снова появятся в «🎾 Игры», если они останутся."
        )
        return
    if result["status"] == "full":
        await callback.message.answer(
            "К сожалению, свободных мест уже меньше, чем ты выбрал."
        )
        return
    if result["status"] == "must_fill_all":
        free_slots = int(result.get("free_slots") or 0)
        total_slots = int(result.get("total_slots") or (result.get("game") or {}).get("total_slots") or 0)
        await callback.message.answer(
            _must_fill_all_text(free_slots, total_slots),
            reply_markup=_last_hour_fill_keyboard(game_id, free_slots) if free_slots > 0 else None,
            parse_mode="HTML",
        )
        return
    if result["status"] not in {"ok", "duplicate"} or not result.get("booking") or not result.get("game"):
        await callback.message.answer("Не удалось записаться. Попробуй ещё раз.")
        return

    booking = result["booking"]
    game = result["game"]
    booking_id = booking["id"]
    # Сумма только из фактически записанных мест в БД — не из callback
    # (иначе подмена book_slots:...:0 давала бы оплату 0 ₽ при 1 месте).
    slots_count = int(booking.get("slots_count") or 1)
    total_price = float(game["price"]) * slots_count
    _invalidate_games_cache()

    payment = result.get("payment")
    is_resume = result["status"] == "duplicate"

    # Уже оплачено / ждёт подтверждения админа — не шлём счёт снова.
    if is_resume and payment and (
        payment.get("status") == "подтверждена"
        or payment.get("player_notified_at") is not None
    ):
        await callback.message.answer(
            "✅ Ты уже записан и оплатил эту игру.\n"
            "Детали — в «📋 Мои записи».",
            reply_markup=main_menu_keyboard(),
        )
        return

    # Платёж в той же транзакции, что бронь; при resume без «ожидает» — создаём.
    if not payment or payment.get("status") != "ожидает":
        payment = await asyncio.wait_for(
            db.get_or_create_pending_payment(booking_id, total_price),
            timeout=10,
        )
    # Resume / доплата: на экране оплаты — сумма открытого счёта, не price*все места.
    if payment and payment.get("status") == "ожидает":
        total_price = float(payment["amount"])
    if not payment:
        await callback.message.answer(
            "❌ Не удалось открыть оплату. Открой «📋 Мои записи» или попробуй ещё раз.",
            reply_markup=main_menu_keyboard(),
        )
        return

    is_training = (game.get("event_type") or "game") == "training"
    event_word = "тренировку" if is_training else "игру"
    when = _format_game_when(game)

    if not is_resume:
        # Сначала ответ игроку — уведомление админу не должно влиять на UX.
        title_bit = ""
        if is_training and game.get("title"):
            title_bit = f" «{_html(game['title'])}»"
        await callback.message.answer(
            f"✅ Ты записан на {event_word}{title_bit} {when}!\n\n"
            f"👥 Мест: <b>{slots_count}</b>\n"
            f"💰 К оплате: <b>{total_price:.0f} ₽</b>\n"
            "📌 Статус заявки: <b>новая</b>\n\n"
            "Посмотреть записи: «📋 Мои записи» в меню",
            reply_markup=main_menu_keyboard(),
            parse_mode="HTML",
        )
        taken = int(result.get("taken") or 0)
        total_slots = int(result.get("total_slots") or game["total_slots"])
        if not is_training and taken and taken < total_slots:
            await callback.message.answer(
                _underfill_booking_notice(taken, total_slots),
                parse_mode="HTML",
            )
        _fire_and_forget(
            _notify_admin_new_booking(
                callback.bot, user, game, booking_id, slots_count, total_price,
            ),
            name=f"admin_new_booking:{booking_id}",
        )
    else:
        await callback.message.answer(
            f"Ты уже записан на эту {event_word} — продолжаем оплату.\n"
            "Отменить можно кнопкой ниже или в «📋 Мои записи».",
            reply_markup=main_menu_keyboard(),
        )

    await _offer_payment(
        callback.message,
        callback.bot,
        user=user,
        payment=payment,
        game=game,
        total_price=total_price,
    )


@router.callback_query(F.data.regexp(r"^buy_more:\d+$"))
async def process_buy_more_ask(callback: CallbackQuery):
    """Из «Мои записи»: докупить места на ту же игру, пока набор не полный."""
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    booking_id = int(callback.data.split(":")[1])
    booking = await db.get_booking_by_id(booking_id)
    if not booking or booking.get("user_id") != user["id"] or booking.get("status") == "отменена":
        await callback.message.answer("Запись не найдена.")
        return

    offer = await db.get_game_slot_offer(int(booking["game_id"]))
    if not offer:
        await callback.message.answer(
            "На этой игре больше нет свободных мест или она уже недоступна."
        )
        return

    game = offer["game"]
    free_slots = offer["free_slots"]
    total_slots = offer["total_slots"]

    if offer["within_last_hour"]:
        await callback.message.answer(
            _must_fill_all_text(free_slots, total_slots) +
            f"\n\n💰 Цена за место: {game['price']} ₽\n"
            f"Итого за {free_slots}: <b>{float(game['price']) * free_slots:.0f} ₽</b>",
            reply_markup=_buy_more_last_hour_keyboard(booking_id, free_slots),
            parse_mode="HTML",
        )
        return

    max_choice = min(MAX_SLOTS_PER_BOOKING, free_slots)
    await callback.message.answer(
        "➕ <b>Докупить места</b>\n"
        f"Свободно на игре: {free_slots} из {total_slots}\n"
        f"У тебя сейчас: {booking.get('slots_count', 1)}\n"
        f"Цена за место: {game['price']} ₽\n\n"
        "Сколько мест добавить?",
        reply_markup=_buy_more_slots_keyboard(booking_id, max_choice),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "buy_more_back")
async def process_buy_more_back(callback: CallbackQuery):
    await callback.answer()
    await _show_my_bookings(callback.message)


@router.callback_query(F.data.startswith("buy_more_slots:"))
async def process_buy_more_confirm(callback: CallbackQuery):
    """Подтверждение докупки мест → увеличиваем бронь и открываем оплату доплаты."""
    try:
        await callback.answer("Добавляю места…")
    except Exception:
        pass
    try:
        await _do_buy_more_confirm(callback)
    except asyncio.TimeoutError:
        logger.error("Таймаут докупки buy_more_slots data=%s", callback.data)
        await callback.message.answer(
            "❌ База данных отвечает слишком долго.\n"
            "Попробуй ещё раз через несколько секунд.",
            reply_markup=main_menu_keyboard(),
        )
    except Exception as e:
        logger.exception("Ошибка докупки buy_more_slots data=%s: %s", callback.data, e)
        await callback.message.answer(
            "❌ Не удалось докупить места из‑за временной ошибки.\n"
            "Попробуй ещё раз или открой «📋 Мои записи».",
            reply_markup=main_menu_keyboard(),
        )


async def _do_buy_more_confirm(callback: CallbackQuery) -> None:
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.message.answer("Некорректные данные.")
        return
    try:
        booking_id = int(parts[1])
        extra_slots = int(parts[2])
    except ValueError:
        await callback.message.answer("Некорректные данные.")
        return
    if extra_slots < 1:
        await callback.message.answer("Некорректное количество мест.")
        return

    # Снимаем inline-клавиатуру, чтобы двойной тап не добавил места дважды.
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    result = await asyncio.wait_for(
        db.increase_booking_slots_safe(
            user_id=user["id"],
            booking_id=booking_id,
            extra_slots=extra_slots,
        ),
        timeout=15,
    )

    if result["status"] == "not_found":
        await callback.message.answer("Запись или игра больше недоступны.")
        return
    if result["status"] == "forbidden":
        await callback.message.answer("Это не твоя запись.")
        return
    if result["status"] == "full":
        await callback.message.answer(
            "Свободных мест уже меньше, чем ты выбрал. Открой «Мои записи» снова."
        )
        return
    if result["status"] == "must_fill_all":
        free_slots = int(result.get("free_slots") or 0)
        total_slots = int(result.get("total_slots") or 0)
        await callback.message.answer(
            _must_fill_all_text(free_slots, total_slots),
            reply_markup=(
                _buy_more_last_hour_keyboard(booking_id, free_slots)
                if free_slots > 0 else None
            ),
            parse_mode="HTML",
        )
        return
    if result["status"] != "ok" or not result.get("payment") or not result.get("game"):
        await callback.message.answer("Не удалось докупить места. Попробуй ещё раз.")
        return

    booking = result["booking"]
    game = result["game"]
    payment = result["payment"]
    added = int(result["extra_slots"])
    # К оплате — сумма текущего «ожидает» (доплата или увеличенный счёт).
    total_price = float(payment["amount"])
    _invalidate_games_cache()

    _fire_and_forget(
        _notify_admin_extra_slots(
            callback.bot, user, game, booking_id, added, total_price,
            int(booking.get("slots_count") or added),
        ),
        name=f"admin_extra_slots:{booking_id}",
    )

    await callback.message.answer(
        f"✅ Добавлено мест: <b>{added}</b>\n"
        f"👥 Всего твоих мест: <b>{booking.get('slots_count', added)}</b>\n"
        f"💰 К оплате сейчас: <b>{total_price:.0f} ₽</b>\n\n"
        "Оплати доплату ниже — администратор подтвердит её в CRM.",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )
    taken = int(result.get("taken") or 0)
    total_slots = int(result.get("total_slots") or game["total_slots"])
    is_training = (game.get("event_type") or "game") == "training"
    if not is_training and taken and taken < total_slots:
        await callback.message.answer(
            _underfill_booking_notice(taken, total_slots),
            parse_mode="HTML",
        )

    await _offer_payment(
        callback.message,
        callback.bot,
        user=user,
        payment=payment,
        game=game,
        total_price=total_price,
    )


async def _notify_admin_extra_slots(
    bot: Bot,
    user: dict,
    game: dict,
    booking_id: int,
    extra_slots: int,
    pay_amount: float,
    total_slots_owned: int,
) -> None:
    game_datetime = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
    )
    text = (
        "➕ <b>Докупка мест</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Имя: {_html(user['name'])}\n"
        f"📞 Телефон: {_html(user['phone'])}\n"
        f"📅 Дата и время: {_html(game_datetime)}\n"
        f"📍 Корт / площадка: {_html(game['location'])}\n"
        f"➕ Добавлено мест: {extra_slots}\n"
        f"👥 Всего мест у игрока: {total_slots_owned}\n"
        f"💰 К оплате (текущий счёт): {pay_amount:.0f} ₽\n"
        f"🆔 ID заявки: {booking_id}"
    )
    try:
        sent = await _send_admin_message(bot, text, parse_mode="HTML")
        if sent is not None:
            try:
                await db.set_booking_admin_extra_notify_message(booking_id, sent.message_id)
            except Exception as e:
                logger.error(
                    "Не удалось сохранить message_id докупки для брони #%s: %s",
                    booking_id, e,
                )
    except Exception as e:
        logger.error("Не удалось уведомить админа о докупке #%s: %s", booking_id, e)


# ---------------------------------------------------------------------------
# Оплата: выбор способа → ссылка / счёт Telegram → отмена
# ---------------------------------------------------------------------------

def _underfill_booking_notice(taken: int, total_slots: int) -> str:
    """Предупреждение перед оплатой, если корт ещё не укомплектован."""
    return (
        "⚠️ <b>Важно: набор на игру ещё не полный</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Сейчас записано: <b>{taken}/{total_slots}</b>\n\n"
        "Если за <b>1 час</b> до начала игры не соберётся полный состав "
        f"(<b>{total_slots}/{total_slots}</b>), запись <b>автоматически отменится</b>, "
        "а оплата будет возвращена.\n\n"
        "За 3 часа до старта мы дополнительно напомним, если мест всё ещё не хватает."
    )


def _payment_header(game: dict, amount: float) -> str:
    is_training = (game.get("event_type") or "game") == "training"
    title_line = ""
    if is_training and game.get("title"):
        title_line = f"<b>{_html(game['title'])}</b>\n"
    return (
        "💳 <b>Оплата</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{title_line}"
        f"📅 {_html(_format_game_when(game))}\n"
        f"📍 {_html(game.get('location'))}\n"
        f"💰 Сумма: <b>{amount:.0f} ₽</b>\n\n"
    )


def _payment_method_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="💳 Оплатить картой",
            callback_data=f"pay_card:{payment_id}",
        )],
        [InlineKeyboardButton(
            text="📱 Оплатить по СБП (QR)",
            callback_data=f"pay_sbp:{payment_id}",
        )],
        [InlineKeyboardButton(
            text="🗑 Отменить заказ",
            callback_data=f"pay_cancel_ask:{payment_id}",
        )],
    ])


def _paid_notify_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✅ Я оплатил",
            callback_data=f"paid_notify:{payment_id}",
        )],
        [InlineKeyboardButton(
            text="🗑 Отменить заказ",
            callback_data=f"pay_cancel_ask:{payment_id}",
        )],
    ])


def _pay_cancel_confirm_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, отменить заказ", callback_data=f"pay_cancel_yes:{payment_id}")],
        [InlineKeyboardButton(text="Нет, оставить", callback_data=f"pay_cancel_no:{payment_id}")],
    ])


async def _offer_payment(
    message: Message,
    bot: Bot,
    *,
    user: dict,
    payment: dict,
    game: dict,
    total_price: float,
) -> None:
    """Выбор способа оплаты: карта или СБП (заглушки)."""
    amount = float(total_price)
    await message.answer(
        _payment_header(game, amount) + "Выбери способ оплаты:",
        reply_markup=_payment_method_keyboard(int(payment["id"])),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("pay_card:"))
async def process_pay_card(callback: CallbackQuery):
    """Заглушка оплаты картой → «Я оплатил» → уведомление админу."""
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_for_user(payment_id, user["id"])
    if not payment or payment.get("status") != "ожидает":
        await callback.message.answer("Платёж не найден или уже обработан.")
        return

    await db.set_payment_method_owned(payment_id, user["id"], "card")
    reference = payment_provider.generate_stub_reference("CARD", payment_id)
    await callback.message.answer(
        "💳 <b>Оплата картой</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Это демо-режим: реальный эквайринг не подключён.\n\n"
        f"Сумма: <b>{float(payment['amount']):.0f} ₽</b>\n"
        f"Референс: <code>{_html(reference)}</code>\n\n"
        "Оплати администратору на месте/переводом и нажми кнопку ниже:",
        reply_markup=_paid_notify_keyboard(payment_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("pay_sbp:"))
async def process_pay_sbp(callback: CallbackQuery):
    """Заглушка СБП с тестовым QR → «Я оплатил» → уведомление админу."""
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_for_user(payment_id, user["id"])
    if not payment or payment.get("status") != "ожидает":
        await callback.message.answer("Платёж не найден или уже обработан.")
        return

    await db.set_payment_method_owned(payment_id, user["id"], "sbp")
    reference = payment_provider.generate_stub_reference("SBP", payment_id)
    qr_payload = payment_provider.build_sbp_payload(float(payment["amount"]), reference)
    qr_bytes = payment_provider.make_qr_image_bytes(qr_payload)

    await callback.message.answer_photo(
        BufferedInputFile(qr_bytes, filename="sbp_qr.png"),
        caption=(
            "📱 <b>Оплата по СБП</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Это демо-режим: реальный СБП не подключён "
            "(ниже тестовый QR).\n\n"
            f"Сумма: <b>{float(payment['amount']):.0f} ₽</b>\n"
            f"Референс: <code>{_html(reference)}</code>\n\n"
            "Оплати администратору на месте/переводом и нажми кнопку ниже:"
        ),
        reply_markup=_paid_notify_keyboard(payment_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("pay_method_sbp:"))
@router.callback_query(F.data.startswith("pay_open:"))
async def process_pay_legacy_online(callback: CallbackQuery):
    """Старые кнопки ЮKassa/PayMaster → выбор карты / СБП."""
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_for_user(payment_id, user["id"])
    if not payment or payment.get("status") != "ожидает":
        await callback.message.answer("Платёж не найден или уже обработан.")
        return
    booking = await db.get_booking_by_id(payment["booking_id"])
    game = await db.get_game_by_id(booking["game_id"]) if booking else None
    if not game:
        await callback.message.answer("Игра не найдена.")
        return
    await _offer_payment(
        callback.message,
        callback.bot,
        user=user,
        payment=payment,
        game=game,
        total_price=float(payment["amount"]),
    )


@router.callback_query(F.data.startswith("pay_cancel_ask:"))
async def process_pay_cancel_ask(callback: CallbackQuery):
    """Отмена с экрана оплаты — предупреждение зависит от факта оплаты."""
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_for_user(payment_id, user["id"])
    if not payment:
        await callback.message.answer("Платёж не найден или это не твоя запись.")
        return

    info = await db.get_booking_cancel_info(payment["booking_id"], user["id"])
    if info["status"] == "not_found":
        await callback.message.answer("Запись не найдена.")
        return
    if info["status"] == "forbidden":
        await callback.message.answer("Это не твоя запись.")
        return
    if info["status"] == "too_late":
        await callback.message.answer(
            "Игра уже началась или была отменена — отменить запись нельзя.",
            reply_markup=main_menu_keyboard(),
        )
        return

    paid = payment.get("status") == "подтверждена"
    refund_window = bool(info.get("refund_window"))
    is_open_unpaid = (
        payment.get("status") == "ожидает"
        and payment.get("player_notified_at") is None
    )
    keep_paid_seats = False
    if is_open_unpaid:
        keep_paid_seats = await db.booking_has_protected_payment(
            int(payment["booking_id"]),
            exclude_payment_id=payment_id,
        )

    if keep_paid_seats:
        text = (
            "Отменить доплату?\n\n"
            "Уже оплаченные места останутся в «Мои записи». "
            "Снимется только неоплаченная докупка."
        )
    elif not paid:
        text = (
            "Точно отменить запись?\n\n"
            "Оплата ещё не поступила — место снова станет свободным."
        )
    elif refund_window:
        text = (
            "⚠️ <b>Ты уже оплатил эту запись</b>\n\n"
            "Не отменяй, если хочешь сохранить место.\n"
            "При отмене более чем за 12 часов до игры оплата будет возвращена "
            "после оформления администратором.\n\n"
            "Точно отменить?"
        )
    else:
        text = (
            "⚠️ <b>Ты уже оплатил эту запись</b>\n\n"
            "Не отменяй, если хочешь сохранить место.\n"
            "До начала игры осталось меньше 12 часов — при отмене "
            "<b>оплата не возвращается</b>, средства будут потеряны.\n\n"
            "Точно отменить?"
        )

    await callback.message.answer(
        text,
        reply_markup=_pay_cancel_confirm_keyboard(payment_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("pay_cancel_no:"))
async def process_pay_cancel_no(callback: CallbackQuery):
    await callback.answer("Запись сохранена.")
    try:
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n✅ Запись сохранена.",
            reply_markup=None,
        )
    except Exception:
        await callback.message.answer("Хорошо, запись остаётся активной.")


@router.callback_query(F.data.startswith("pay_cancel_yes:"))
async def process_pay_cancel_yes(callback: CallbackQuery):
    await callback.answer("Отменяю…")
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_for_user(payment_id, user["id"])
    if not payment:
        await callback.message.answer("Платёж не найден или это не твоя запись.")
        return

    booking_id = int(payment["booking_id"])
    result = await db.cancel_payment_or_booking_owned(
        booking_id, user["id"], payment_id,
    )
    if result["status"] == "not_found":
        await callback.message.answer("Запись не найдена.")
        return
    if result["status"] == "forbidden":
        await callback.message.answer("Это не твоя запись.")
        return
    if result["status"] == "payment_pending_confirm":
        await callback.message.answer(
            "Ты уже сообщил об оплате. Отменить запись можно только после того, "
            "как администратор подтвердит оплату.",
        )
        return
    if result["status"] == "too_late":
        await callback.message.answer(
            "Игра уже началась или была отменена — отменить запись нельзя.",
            reply_markup=main_menu_keyboard(),
        )
        return
    if result["status"] == "extra_cancelled":
        _invalidate_games_cache()
        await _cleanup_admin_notifies_after_cancel(callback.bot, result)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        kept = int(result.get("slots_kept") or 1)
        removed = int(result.get("extra_slots_removed") or 0)
        await callback.message.answer(
            "✅ Доплата отменена.\n\n"
            f"Твоя запись сохранена: <b>{kept}</b> "
            f"{'место' if kept == 1 else 'места' if kept < 5 else 'мест'}"
            + (f" (снято неоплаченных: {removed})" if removed else "")
            + ".\nСмотри в «📋 Мои записи».",
            reply_markup=main_menu_keyboard(),
            parse_mode="HTML",
        )
        return
    if result["status"] != "ok":
        await callback.message.answer("Не удалось отменить запись. Попробуй позже.")
        return

    _invalidate_games_cache()
    await _cleanup_admin_notifies_after_cancel(callback.bot, result)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if result.get("refund_eligible") and result.get("payment"):
        await _notify_refund(callback, user, booking_id, result.get("game"), result["payment"])
    elif not result.get("refund_window") and result.get("had_payment"):
        await callback.message.answer(
            "Ваша запись отменена. К сожалению, вернуть оплату не получится — до начала "
            "игры остаётся меньше 12 часов, а по правилам сервиса возврат возможен только "
            "при отмене заранее.\n\n"
            "Ничего страшного — в разделе «🎾 Игры» можно выбрать другую игру, будем рады "
            "видеть тебя снова! 🎾",
            reply_markup=main_menu_keyboard(),
        )
    else:
        await callback.message.answer(
            "Запись отменена. Если передумаешь — загляни в «🎾 Игры».",
            reply_markup=main_menu_keyboard(),
        )


@router.callback_query(F.data.startswith("paid_notify:"))
async def process_paid_notify(callback: CallbackQuery):
    """Игрок нажал «Я оплатил» — уведомляем его и админа; подтверждение в CRM."""
    await callback.answer("Спасибо! Администратор проверит оплату.")
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    payment_id = int(callback.data.split(":")[1])
    # Именно с этого момента платёж считается "новым" для бейджа "+N" рядом
    # с "Оплаты" в CRM — до этого клика он уже существовал (создаётся сразу
    # при записи на игру), но игрок ещё не заявлял об оплате.
    payment = await db.mark_payment_notified_owned(payment_id, user["id"])
    if not payment:
        await callback.message.answer("Платёж не найден или это не твоя запись.")
        return
    if payment.get("_already_notified"):
        await callback.message.answer(
            "Мы уже получили твоё уведомление об оплате. "
            "Дождись подтверждения администратора."
        )
        return

    # Скрываем игру из списка для остальных, пока админ не подтвердит.
    _invalidate_games_cache()

    await callback.message.answer(_payment_awaiting_admin_text())

    try:
        await _notify_admin_player_paid(
            callback.bot,
            user=user,
            payment=payment,
            telegram_user=callback.from_user,
            source=payment.get("method") or "перевод / на месте",
        )
    except Exception as e:
        logger.error("Не удалось уведомить админа об оплате #%s: %s", payment_id, e)


def _payment_awaiting_admin_text() -> str:
    return (
        "Спасибо! Оплата будет подтверждена администратором в ближайшее время."
    )


async def _notify_admin_player_paid(
    bot: Bot,
    *,
    user: dict,
    payment: dict,
    telegram_user,
    source: str = "оплата",
    extra_lines: Optional[str] = None,
) -> None:
    """Личное сообщение админу об оплате — имя + @username."""
    display_name = user.get("name") or getattr(telegram_user, "full_name", None) or "Игрок"
    username = getattr(telegram_user, "username", None)
    username_part = f" (@{_html(username)})" if username else ""
    tg_id = getattr(telegram_user, "id", None) or user.get("telegram_id") or "—"
    method = payment.get("method") or source
    method_label = {
        "card": "карта",
        "sbp": "СБП",
        "bank_card": "карта",
    }.get(str(method), str(method))
    text = (
        "💰 <b>Игрок оплатил игру</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Пользователь {_html(display_name)}{username_part} оплатил "
        f"бронь №{payment['booking_id']}\n\n"
        f"🆔 Telegram ID: <code>{_html(tg_id)}</code>\n"
        f"🆔 Платёж #{payment['id']}\n"
        f"💳 Способ: {_html(method_label)}\n"
        f"💰 Сумма: {_html(payment['amount'])} ₽\n"
    )
    if extra_lines:
        text += f"{extra_lines}\n"
    text += "\nПроверь и подтверди оплату в CRM (раздел «Оплаты»)."
    await _send_admin_message(bot, text, parse_mode="HTML")


# ---------------------------------------------------------------------------
# Мои записи и отмена
# ---------------------------------------------------------------------------

@router.message(Command("my_bookings"))
async def cmd_my_bookings(message: Message):
    await _show_my_bookings(message)


async def _notify_refund(callback: CallbackQuery, user: dict, booking_id: int, game: Optional[dict], payment: dict) -> None:
    """Уведомляет игрока и админа о возврате оплаты (>12ч до игры, была
    подтверждённая оплата) и пишет об этом в общий журнал admin_logs —
    вызывается после успешной отмены брони, поэтому ошибки здесь не должны
    влиять на уже случившуюся отмену, только логируются."""
    await callback.message.answer(
        "Ваша запись отменена. Оплата будет возвращена на ваш счёт в ближайшее время.\n\n"
        "А чтобы не терять форму — заходи в раздел «🎾 Игры»: там уже ждут другие свободные "
        "корты и удобное время, обязательно найдётся что-то подходящее. До встречи на площадке! 🙌"
    )

    game_dt = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
        if game else "—"
    )
    location = game["location"] if game else "—"
    amount = float(payment["amount"])
    username = callback.from_user.username
    username_part = f" (@{username})" if username else ""

    try:
        await _send_admin_message(
            callback.bot,
            "❌ <b>Отмена записи с возвратом оплаты</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Пользователь {_html(user['name'])}{username_part} отменил запись "
            f"на корт (бронь №{booking_id}) более чем за 12 часов до начала\n\n"
            f"📅 {_html(game_dt)}\n"
            f"📍 {_html(location)}\n"
            f"💰 К возврату: {amount:.0f} ₽\n\n"
            "Статус оплаты изменён на «возврат» — оформите возврат средств игроку.",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Не удалось уведомить админа об отмене брони №%s: %s", booking_id, e)

    description = (
        f"Возврат оплаты {amount:.0f} ₽ по брони №{booking_id}: пользователь "
        f"{user['name']}{username_part} отменил запись на игру {game_dt} "
        "более чем за 12 часов до начала"
    )
    try:
        await db.log_action(
            action="refund",
            entity_type="payment",
            entity_id=payment["id"],
            description=description,
            old_value="подтверждена",
            new_value="возврат",
        )
    except Exception as e:
        logger.error("Не удалось записать возврат оплаты #%s в журнал: %s", payment["id"], e)


def _cancel_confirm_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, отменить", callback_data=f"cancel_yes:{booking_id}")],
        [InlineKeyboardButton(text="Нет, оставить", callback_data=f"cancel_no:{booking_id}")],
    ])


@router.callback_query(F.data.regexp(r"^cancel_ask:\d+$"))
@router.callback_query(F.data.regexp(r"^cancel:\d+$"))
async def process_cancel_ask(callback: CallbackQuery):
    """Первый шаг отмены: спрашиваем подтверждение. Если до игры <12ч —
    доброжелательно предупреждаем, что возврата не будет. Если игрок уже
    нажал «Я оплатил», а админ ещё не подтвердил — отмену блокируем.

    cancel:<id> оставлен для кнопок «Не смогу» в напоминаниях; cancel_ask —
    из «Мои записи». Фильтры с regexp, чтобы не перехватывать cancel_yes/no."""
    await callback.answer()
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    booking_id = int(callback.data.split(":")[1])
    info = await db.get_booking_cancel_info(booking_id, user["id"])

    if info["status"] == "not_found":
        await callback.message.answer("Запись не найдена.")
        return
    if info["status"] == "forbidden":
        await callback.message.answer("Это не твоя запись.")
        return

    if info["status"] == "too_late":
        await callback.message.answer(
            "Игра уже началась или была отменена — отменить запись нельзя.",
            reply_markup=main_menu_keyboard(),
        )
        return
    if info.get("payment_pending_confirm"):
        await callback.message.answer(
            "Ты уже сообщил об оплате. Отменить запись можно только после того, "
            "как администратор подтвердит оплату в CRM.",
        )
        return

    game = info.get("game") or {}
    game_dt = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
        if game.get("game_date") else "—"
    )
    location = _html(game.get("location") or "—")

    if info.get("had_confirmed_payment"):
        if info.get("refund_window"):
            text = (
                f"⚠️ <b>Ты уже оплатил</b> запись на <b>{_html(game_dt)}</b>\n"
                f"📍 {location}\n\n"
                "Не отменяй, если хочешь сохранить место.\n"
                "При отмене более чем за 12 часов оплата будет возвращена "
                "после оформления администратором.\n\n"
                "Точно отменить?"
            )
        else:
            text = (
                f"⚠️ <b>Ты уже оплатил</b> запись на <b>{_html(game_dt)}</b>\n"
                f"📍 {location}\n\n"
                "Не отменяй, если хочешь сохранить место.\n"
                "До начала меньше 12 часов — при отмене "
                "<b>оплата не возвращается</b>, средства будут потеряны.\n\n"
                "Точно отменить?"
            )
    elif info.get("refund_window"):
        text = (
            f"Точно отменить запись на <b>{_html(game_dt)}</b>?\n"
            f"📍 {location}"
        )
    else:
        text = (
            f"Точно отменить запись на <b>{_html(game_dt)}</b>?\n"
            f"📍 {location}\n\n"
            "⚠️ До начала игры осталось меньше 12 часов. Если уже оплатишь "
            "и потом отменишь — вернуть оплату будет нельзя."
        )

    await callback.message.answer(
        text,
        reply_markup=_cancel_confirm_keyboard(booking_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("cancel_no:"))
async def process_cancel_no(callback: CallbackQuery):
    await callback.answer("Запись сохранена.")
    try:
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n✅ Запись сохранена.",
            reply_markup=None,
        )
    except Exception:
        await callback.message.answer("Хорошо, запись остаётся активной.")


@router.callback_query(F.data.startswith("cancel_yes:"))
async def process_cancel_yes(callback: CallbackQuery):
    await callback.answer("Отменяю…")
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.message.answer(
            "❌ Сначала заполни анкету.\nОтправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    booking_id = int(callback.data.split(":")[1])

    # Отмена проверяет владельца записи внутри транзакции — раньше можно было
    # отменить чужую запись, зная её booking_id (IDOR). Дополнительно решает,
    # положен ли возврат оплаты (>12ч до игры + была подтверждённая оплата)
    # и блокирует отмену, пока админ не подтвердил оплату после «Я оплатил».
    result = await db.cancel_booking_owned(booking_id, user["id"])

    if result["status"] == "not_found":
        await callback.message.answer("Запись не найдена.")
        return
    if result["status"] == "forbidden":
        await callback.message.answer("Это не твоя запись.")
        return
    if result["status"] == "payment_pending_confirm":
        await callback.message.answer(
            "Ты уже сообщил об оплате. Отменить запись можно только после того, "
            "как администратор подтвердит оплату.",
        )
        return
    if result["status"] == "too_late":
        await callback.message.answer(
            "Игра уже началась или была отменена — отменить запись нельзя.",
            reply_markup=main_menu_keyboard(),
        )
        return
    if result["status"] != "ok":
        await callback.message.answer("Не удалось отменить запись. Попробуй позже.")
        return

    _invalidate_games_cache()
    await _cleanup_admin_notifies_after_cancel(callback.bot, result)

    try:
        await callback.message.edit_text(
            (callback.message.text or "") + "\n\n❌ ОТМЕНЕНО",
            reply_markup=None,
        )
    except Exception:
        pass

    if result.get("refund_eligible") and result.get("payment"):
        await _notify_refund(callback, user, booking_id, result.get("game"), result["payment"])
    elif not result.get("refund_window") and result.get("had_payment"):
        # Игра начинается меньше чем через 12 часов — по правилам возврат не
        # положен, но игрок был оплатившим (had_payment), поэтому явно
        # объясняем, почему денег не будет, а не молчим об этом.
        await callback.message.answer(
            "Ваша запись отменена. К сожалению, вернуть оплату не получится — до начала "
            "игры остаётся меньше 12 часов, а по правилам сервиса возврат возможен только "
            "при отмене заранее.\n\n"
            "Ничего страшного — в разделе «🎾 Игры» можно выбрать другую игру, будем рады "
            "видеть тебя снова! 🎾"
        )
    else:
        await callback.message.answer(
            "Запись отменена. Если передумаешь — загляни в «🎾 Игры».",
            reply_markup=main_menu_keyboard(),
        )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Сброс FSM: анкета или сообщение администратору."""
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        user = await _require_registered(message.from_user.id)
        if user:
            await message.answer(
                "Действие отменено.",
                reply_markup=main_menu_keyboard(),
            )
            await show_main_menu(message, user)
        else:
            await message.answer("Действие отменено. Отправь /start, чтобы начать заново.")
    else:
        await message.answer(
            "Чтобы отменить запись на игру, открой «📋 Мои записи» в меню.",
            reply_markup=main_menu_keyboard(),
        )


@router.message()
async def fallback_message(message: Message, state: FSMContext):
    """Подсказывает пользователю, как двигаться по меню, если он отправил сообщение вне сценария."""
    current_state = await state.get_state()
    if current_state is not None:
        return

    if not message.text:
        return

    if message.text.startswith('/'):
        await message.answer(
            "ℹ️ Я понимаю команды /start, /menu, /myprofile, /games, /my_bookings, /help и /cancel.\n"
            "Для навигации используй кнопки меню ниже.",
            reply_markup=main_menu_keyboard(),
        )
        return

    if message.text in MENU_BUTTONS:
        return

    await message.answer(
        "🧭 Используй кнопки ниже или отправь /start, чтобы открыть меню.",
        reply_markup=main_menu_keyboard(),
    )


# ---------------------------------------------------------------------------
# Напоминания за 24 часа и за 2 часа до игры
# ---------------------------------------------------------------------------

def _refund_deadline_str(game: dict) -> str:
    """Момент, после которого отмена уже без возврата (за 12 часов до старта)."""
    game_time = game["game_time"]
    if hasattr(game_time, "strftime"):
        start = datetime.combine(game["game_date"], game_time)
    else:
        # asyncpg иногда отдаёт time как datetime / str
        start = datetime.combine(game["game_date"], datetime.strptime(str(game_time)[:8], "%H:%M:%S").time())
    deadline = start - timedelta(hours=12)
    return deadline.strftime("%d.%m.%Y в %H:%M")


async def _send_reminder_batch(
    bot: Bot, games: list, label: str, mark_sent, *, include_refund_notice: bool = False,
) -> None:
    """Рассылает напоминание с кнопкой «Не смогу» участникам списка игр и
    помечает игру как обработанную. Кнопка использует существующий callback
    cancel:<booking_id> (см. process_cancel_ask) — сначала подтверждение,
    затем та же логика отмены, что и в «Мои записи».

    include_refund_notice — для напоминания за 24 часа: предупреждаем, что
    отмена менее чем за 12 часов до игры уже без возврата, и указываем
    крайнее время (старт минус 12 часов)."""
    for game in games:
        participants = await db.get_participants_for_game(game["id"])
        game_dt = f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
        is_training = (game.get("event_type") or "game") == "training"
        event_word = "тренировка" if is_training else "игра в падел"
        text = (
            f"⏰ <b>Напоминание!</b> {label} у тебя {event_word}:\n"
        )
        if is_training and game.get("title"):
            text += f"<b>{_html(game['title'])}</b>\n"
        text += (
            f"📅 {_html(game_dt)}\n"
            f"📍 {_html(game['location'])}\n\n"
            "Если не сможешь прийти — нажми «Не смогу», место освободится "
            "для других игроков."
        )
        if include_refund_notice:
            deadline = _refund_deadline_str(game)
            text += (
                "\n\n⚠️ Важно: при отмене менее чем за 12 часов до начала "
                f"{'тренировки' if is_training else 'игры'} "
                f"возврат средств невозможен. Крайний срок для отмены с возвратом — "
                f"<b>{_html(deadline)}</b>."
            )
        text += "\n\nДо встречи на корте! 🎾"

        def _reminder_keyboard(booking_id: int) -> InlineKeyboardMarkup:
            # Клавиатура своя для каждого участника — у каждого свой
            # booking_id, чтобы кнопка отменяла именно ЕГО заявку.
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Не смогу", callback_data=f"cancel:{booking_id}")]
            ])

        # Bot.send_message — настоящая корутина (в отличие от message.answer),
        # поэтому её можно безопасно передавать в asyncio.gather напрямую.
        sends = [
            bot.send_message(
                p["telegram_id"], text, reply_markup=_reminder_keyboard(p["booking_id"]), parse_mode="HTML"
            )
            for p in participants
        ]
        results = await asyncio.gather(*sends, return_exceptions=True)
        sent_ok = 0
        for p, result in zip(participants, results):
            if isinstance(result, Exception):
                logger.error(
                    "Не удалось отправить напоминание пользователю %s: %s",
                    p["telegram_id"], result,
                )
            else:
                sent_ok += 1
        # Не помечаем «отправлено», если никто не получил — иначе окно потеряно.
        if sent_ok > 0:
            await mark_sent(game["id"])
        else:
            logger.warning(
                "Напоминание по игре #%s не доставлено никому — флаг не ставим",
                game["id"],
            )


async def _process_underfill_warnings(bot: Bot) -> None:
    """За ~3 часа до игры: если состав неполный — предупредить записавшихся."""
    games = await db.get_games_needing_underfill_warn_3h()
    for game in games:
        taken = int(game.get("taken") or 0)
        total = int(game["total_slots"])
        game_dt = f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
        text = (
            "⚠️ <b>Набор на игру ещё не полный</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📅 {_html(game_dt)}\n"
            f"📍 {_html(game['location'])}\n"
            f"👥 Сейчас записано: <b>{taken}/{total}</b>\n\n"
            "Если за <b>1 час</b> до начала не соберётся полный состав "
            f"(<b>{total}/{total}</b>), запись <b>автоматически отменится</b>, "
            "а оплатившим игрокам будет оформлен возврат.\n\n"
            "Пригласи друзей или посмотри другие игры в разделе «🎾 Игры»."
        )
        participants = await db.get_participants_for_game(game["id"])
        sends = [
            bot.send_message(p["telegram_id"], text, parse_mode="HTML")
            for p in participants
        ]
        results = await asyncio.gather(*sends, return_exceptions=True)
        for p, result in zip(participants, results):
            if isinstance(result, Exception):
                logger.error(
                    "Не удалось отправить предупреждение о недоборе user=%s: %s",
                    p["telegram_id"], result,
                )
        await db.mark_underfill_warn_3h_sent(game["id"])
        try:
            await _send_admin_message(
                bot,
                "⚠️ <b>Недобор на игру (за 3 часа)</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📅 {game_dt}\n"
                f"📍 {_html(game['location'])}\n"
                f"👥 {taken}/{total}\n\n"
                "Игрокам отправлено предупреждение. Если за час до старта "
                "состав не станет полным — игра отменится автоматически.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Не удалось уведомить админа о недоборе игры #%s: %s", game["id"], e)


async def _process_underfill_cancels(bot: Bot) -> None:
    """За ~1 час до игры: если состав всё ещё неполный — отменить и вернуть оплату."""
    games = await db.get_games_needing_underfill_cancel_1h()
    for game in games:
        result = await db.cancel_underfilled_game(game["id"])
        if result["status"] != "ok":
            continue
        _invalidate_games_cache()
        game_row = result["game"] or game
        taken = int(result.get("taken") or game.get("taken") or 0)
        total = int(game_row["total_slots"])
        game_dt = (
            f"{game_row['game_date'].strftime('%d.%m.%Y')} "
            f"в {str(game_row['game_time'])[:5]}"
        )
        for item in result["cancelled"]:
            if item["refunded"]:
                amount = item["amount"]
                amount_line = (
                    f"\n💰 Возврат оплаты: <b>{amount:.0f} ₽</b> — "
                    "средства вернутся в ближайшее время."
                    if amount is not None else
                    "\n💰 Возврат оплаты будет оформлен в ближайшее время."
                )
            else:
                amount_line = ""
            text = (
                "❌ <b>Игра отменена: не собрался полный состав</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📅 {_html(game_dt)}\n"
                f"📍 {_html(game_row['location'])}\n"
                f"👥 Было записано: <b>{taken}/{total}</b>\n"
                f"{amount_line}\n\n"
                "Запишись на другую игру в разделе «🎾 Игры» — "
                "там уже ждут свободные корты."
            )
            try:
                await bot.send_message(item["telegram_id"], text, parse_mode="HTML")
            except Exception as e:
                logger.error(
                    "Не удалось уведомить игрока %s об автоотмене: %s",
                    item["telegram_id"], e,
                )

        refund_count = sum(1 for x in result["cancelled"] if x["refunded"])
        try:
            await _send_admin_message(
                bot,
                "❌ <b>Автоотмена игры (недобор)</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📅 {game_dt}\n"
                f"📍 {_html(game_row['location'])}\n"
                f"👥 Состав: {taken}/{total}\n"
                f"📋 Отменено записей: {len(result['cancelled'])}\n"
                f"💸 К возврату оплат: {refund_count}\n\n"
                "Проверь раздел «Оплаты» в CRM и оформи возвраты со статусом «возврат».",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Не удалось уведомить админа об автоотмене #%s: %s", game["id"], e)

        try:
            await db.log_action(
                action="cleanup",
                entity_type="game",
                entity_id=game["id"],
                description=(
                    f"Автоотмена игры #{game['id']} ({game_dt}) из‑за недобора "
                    f"{taken}/{total}: отменено записей {len(result['cancelled'])}, "
                    f"возвратов {refund_count}."
                ),
                old_value=f"{taken}/{total}",
                new_value="отменена (недобор)",
            )
        except Exception as e:
            logger.error("Не удалось записать admin_log автоотмены #%s: %s", game["id"], e)


def _unpaid_timeout_minutes() -> int:
    try:
        return max(1, int(os.getenv("UNPAID_PAYMENT_TIMEOUT_MINUTES", "3")))
    except ValueError:
        return 5


def _format_auto_cancel_order_text(game: dict) -> str:
    """Текст игроку при автоотмене неоплаченного заказа."""
    game_dt = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
    )
    is_training = (game.get("event_type") or "game") == "training"
    if is_training and game.get("title"):
        target = f"тренировку «{_html(game['title'])}» ({_html(game_dt)})"
    elif is_training:
        target = f"тренировку {_html(game_dt)}"
    else:
        target = f"игру {_html(game_dt)}"
    return (
        f"Ваш заказ на {target} был отменён автоматически.\n\n"
        "Ждём вас снова!"
    )


async def process_unpaid_payment_timeouts(bot: Bot) -> None:
    """Отменяет неоплаченные счета старше UNPAID_PAYMENT_TIMEOUT_MINUTES
    и пишет игроку. Запускается каждую минуту."""
    minutes = _unpaid_timeout_minutes()
    try:
        payment_ids = await db.list_expired_unpaid_payment_ids(minutes)
    except Exception as e:
        logger.error("Не удалось получить просроченные оплаты: %s", e)
        return

    for payment_id in payment_ids:
        try:
            result = await db.expire_unpaid_payment(
                payment_id, older_than_minutes=minutes,
            )
        except Exception as e:
            logger.error("expire_unpaid_payment #%s failed: %s", payment_id, e)
            continue
        if not result:
            continue

        _invalidate_games_cache()

        await _cleanup_admin_notifies_after_cancel(bot, {
            "status": "extra_cancelled" if result.get("mode") == "extra" else "ok",
            "payment_deleted": True,
            "had_payment": False,
            "admin_notify_message_id": result.get("admin_notify_message_id"),
            "admin_extra_notify_message_id": result.get("admin_extra_notify_message_id"),
        })

        try:
            await bot.send_message(
                result["telegram_id"],
                _format_auto_cancel_order_text(result["game"]),
                parse_mode="HTML",
                reply_markup=main_menu_keyboard(),
            )
        except Exception as e:
            logger.error(
                "Не удалось уведомить игрока %s об автоотмене оплаты #%s: %s",
                result.get("telegram_id"), payment_id, e,
            )

        try:
            game = result["game"]
            game_dt = (
                f"{game['game_date'].strftime('%d.%m.%Y')} "
                f"в {str(game['game_time'])[:5]}"
            )
            mode = "доплата" if result.get("mode") == "extra" else "заказ"
            await db.log_action(
                action="cleanup",
                entity_type="payment",
                entity_id=payment_id,
                description=(
                    f"Автоотмена неоплаченного {mode} #{payment_id} "
                    f"(бронь #{result['booking_id']}, {game_dt}) "
                    f"по таймауту {minutes} мин."
                ),
                old_value="ожидает",
                new_value="отменён (таймаут оплаты)",
            )
        except Exception as e:
            logger.error("Не удалось записать лог автоотмены оплаты #%s: %s", payment_id, e)


async def send_reminders(bot: Bot):
    """Запускается планировщиком (см. main()/app.py:run_bot) каждые 15
    минут: напоминания за 24/2 часа + предупреждение/автоотмена при недоборе."""
    games_24h = await db.get_games_needing_reminder_24h()
    await _send_reminder_batch(
        bot, games_24h, "Через 24 часа", db.mark_reminder_24h_sent,
        include_refund_notice=True,
    )

    games_2h = await db.get_games_needing_reminder_2h()
    await _send_reminder_batch(bot, games_2h, "Через 2 часа", db.mark_reminder_2h_sent)

    await _process_underfill_warnings(bot)
    await _process_underfill_cancels(bot)


def _make_bot_scheduler_spawner(loop: asyncio.AbstractEventLoop):
    """Запуск фоновых корутин бота без future.result(): иначе при таймауте
    задача продолжает крутиться, а через минуту стартует ещё одна."""
    running = {"reminders": False, "unpaid": False}

    def spawn(name: str, coro_factory):
        if running.get(name):
            logger.warning("Пропуск фоновой задачи %s — предыдущая ещё выполняется", name)
            return

        async def _runner():
            running[name] = True
            try:
                await coro_factory()
            except Exception as e:
                logger.error("Ошибка фоновой задачи %s: %s", name, e)
            finally:
                running[name] = False

        try:
            asyncio.run_coroutine_threadsafe(_runner(), loop)
        except Exception as e:
            running[name] = False
            logger.error("Не удалось запустить фоновую задачу %s: %s", name, e)

    return spawn


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN. Проверьте файл .env")

    # Пул соединений asyncpg создаётся один раз на процесс бота, до старта
    # диспетчера — все обработчики дальше просто берут соединения из пула.
    await db.get_pool()
    # Держит БД "тёплой", чтобы /start и первое сообщение после паузы не
    # ждали холодный старт Neon (~5с) — см. db.keepalive_loop.
    asyncio.create_task(db.keepalive_loop())

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await setup_bot_commands(bot)

    # Планировщик — в отдельном потоке (BackgroundScheduler), а не в event
    # loop бота (как было раньше с AsyncIOScheduler): даже если задача
    # напоминаний зависнет или БД будет медленно отвечать, это не заблокирует
    # обработку сообщений пользователей ботом. Интервал 15 минут (а не 1 час,
    # как было для 24ч-напоминания) — чтобы надёжно попадать в более узкое
    # окно 2-часового напоминания (1ч45м-2ч15м).
    loop = asyncio.get_running_loop()
    spawn_job = _make_bot_scheduler_spawner(loop)
    # Часовой пояс планировщика — Москва (как и вся логика игр/возвратов).
    scheduler = BackgroundScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        lambda: spawn_job("reminders", lambda: send_reminders(bot)),
        "interval",
        minutes=15,
    )
    scheduler.add_job(
        lambda: spawn_job("unpaid", lambda: process_unpaid_payment_timeouts(bot)),
        "interval",
        minutes=1,
    )
    scheduler.start()

    try:
        # Если задан WEBHOOK_URL - используем вебхуки, иначе long polling
        if WEBHOOK_URL:
            if not WEBHOOK_SECRET_TOKEN:
                raise RuntimeError(
                    "WEBHOOK_URL задан, но нет WEBHOOK_SECRET_TOKEN / FLASK_SECRET_KEY "
                    "для secret_token вебхука."
                )
            logger.info(f"Запуск бота с вебхуком: {WEBHOOK_URL}{WEBHOOK_PATH}")
            await bot.set_webhook(
                url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
                secret_token=WEBHOOK_SECRET_TOKEN,
            )
            logger.info("Бот запущен с вебхуком...")
        else:
            logger.info("Запуск бота с long polling...")
            logger.info("Бот запущен...")
            await dp.start_polling(bot)
    finally:
        scheduler.shutdown(wait=False)
        await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
