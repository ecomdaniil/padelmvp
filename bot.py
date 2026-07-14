"""
bot.py
------
Telegram-бот для игроков в падел.

Что умеет:
- /start — расширенная анкета (имя, возраст, город, опыт, инвентарь, правила, телефон)
- показывает список ближайших игр и позволяет записаться
- /my_bookings — список своих записей с возможностью отмены
- автоматически шлёт напоминание за 24 часа до игры

Запуск (когда виртуальное окружение активировано):
    python bot.py
"""

import asyncio
import logging
import os
import re
import time
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, Dict

from dotenv import load_dotenv
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    CallbackQuery,
    TelegramObject,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import cache
import database_async as db
from bot_content import (
    BTN_ABOUT_PADEL,
    BTN_COACHES,
    BTN_CONTACT_ADMIN,
    BTN_GAMES,
    BTN_MAIN_MENU,
    BTN_MY_BOOKINGS,
    BTN_STATS,
    COACHES,
    MENU_BUTTONS,
    PADEL_INFO_TEXT,
)

load_dotenv()

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")

# По умолчанию логируем только ошибки — это отдельно настраиваемо через .env,
# если для отладки понадобится более подробный вывод (INFO/DEBUG).
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.ERROR))
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")

GAMES_CACHE_KEY = "games:upcoming_with_slots"
GAMES_CACHE_TTL = 30  # секунд — баланс между свежестью данных и нагрузкой на БД
LEVELS_CACHE_KEY = "levels:list"
LEVELS_CACHE_TTL = 3600

RATE_LIMIT_PER_SECOND = int(os.getenv("BOT_RATE_LIMIT_PER_SECOND", "10"))


class ThrottlingMiddleware(BaseMiddleware):
    """Простой rate limiting: не более RATE_LIMIT_PER_SECOND событий
    (сообщений/нажатий кнопок) от одного пользователя за скользящее окно
    в 1 секунду. Защищает бота и БД от флуда/спама одним пользователем."""

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
    waiting_for_city = State()
    waiting_for_level = State()
    waiting_for_inventory = State()
    waiting_for_rules = State()
    waiting_for_phone = State()


class AdminContact(StatesGroup):
    waiting_for_message = State()


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Постоянная клавиатура главного меню."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_GAMES), KeyboardButton(text=BTN_MY_BOOKINGS)],
            [KeyboardButton(text=BTN_STATS), KeyboardButton(text=BTN_COACHES)],
            [KeyboardButton(text=BTN_ABOUT_PADEL)],
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
        f"📛 Имя: {user['name']}\n"
        f"🎂 Возраст: {user.get('age') or '—'}\n"
        f"🏙 Город: {user.get('city') or '—'}\n"
        f"🎾 Опыт: {user['level']}\n"
        f"🎒 Свой инвентарь: {inventory}\n"
        f"📖 Нужны правила: {rules}\n"
        f"📞 Телефон: {user['phone']}\n"
    )


def _safe_text(message: Message) -> str:
    """Возвращает текст сообщения без исключения для пустых/не-текстовых обновлений."""
    return (message.text or "").strip()


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
        f"👋 Привет, <b>{user['name']}</b>!\n\n"
        "📌 Выберите раздел ниже для быстрого доступа:\n"
        "• 🎾 Игры — посмотреть и записаться\n"
        "• 📋 Мои записи — посмотреть статус заявок\n"
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
        "📝 <b>Вопрос 1 из 7</b>\n\n"
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
            city=data.get("city"),
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
            city=data.get("city"),
            has_inventory=data.get("has_inventory"),
            needs_rules=data.get("needs_rules"),
        )
        title = "Спасибо, анкета заполнена! ✅"

    await state.clear()

    inventory_text = "Да ✅" if data.get("has_inventory") else "Нет"
    rules_text = "Да" if data.get("needs_rules") else "Нет"

    await message.answer(
        f"{title}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📛 Имя: {data['name']}\n"
        f"🎂 Возраст: {data.get('age')}\n"
        f"🏙 Город: {data.get('city')}\n"
        f"🎾 Опыт: {data['level']}\n"
        f"🎒 Инвентарь: {inventory_text}\n"
        f"📖 Правила объяснены: {rules_text}\n"
        f"📞 Телефон: {data['phone']}\n\n"
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


@router.callback_query(F.data == "edit_profile")
async def edit_profile(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await _start_questionnaire(callback.message, state, is_edit=True)


@router.callback_query(F.data == "show_games")
async def show_games_from_profile(callback: CallbackQuery):
    await callback.answer()
    await cmd_games(callback.message)


@router.message(StateFilter(RegistrationForm.waiting_for_name))
async def process_name(message: Message, state: FSMContext):
    name = _safe_text(message)
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
        "📝 <b>Вопрос 2 из 7</b>\n\n"
        "🎂 Сколько тебе лет?\n\n"
        "<i>Введи число, например: 28</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_age))
async def process_age(message: Message, state: FSMContext):
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
    await state.set_state(RegistrationForm.waiting_for_city)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 3 из 7</b>\n\n"
        "🏙 Из какого ты города?",
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_city))
async def process_city(message: Message, state: FSMContext):
    city = _safe_text(message)
    if len(city) < 2:
        await message.answer("❌ Укажи название города, например: Москва или Санкт-Петербург")
        return

    await state.update_data(city=city)
    await state.set_state(RegistrationForm.waiting_for_level)
    await message.answer(
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>Вопрос 4 из 7</b>\n\n"
        "🎾 Какой у тебя опыт игры в падел?\n\n"
        "<i>Выбери вариант из кнопок ниже:</i>",
        reply_markup=EXPERIENCE_KEYBOARD,
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_level))
async def process_level(message: Message, state: FSMContext):
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
        "📝 <b>Вопрос 5 из 7</b>\n\n"
        "🎒 Есть ли у тебя свой инвентарь (ракетка, мячи)?",
        reply_markup=YES_NO_KEYBOARD,
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_inventory))
async def process_inventory(message: Message, state: FSMContext):
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
        "📝 <b>Вопрос 6 из 7</b>\n\n"
        "📖 Нужно ли объяснить правила игры в падел?",
        reply_markup=YES_NO_KEYBOARD,
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_rules))
async def process_rules(message: Message, state: FSMContext):
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
        "📝 <b>Вопрос 7 из 7 (последний!)</b>\n\n"
        "📞 Оставь свой номер телефона.\n\n"
        "<i>Введи только цифры, например: 79001234567</i>",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML",
    )


@router.message(StateFilter(RegistrationForm.waiting_for_phone))
async def process_phone(message: Message, state: FSMContext):
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
    открытии раздела «Игры». Кэш инвалидируется сразу после записи/отмены."""
    cached = cache.get(GAMES_CACHE_KEY)
    if cached is not None:
        return cached
    games = await db.get_upcoming_games_with_slots()
    cache.set(GAMES_CACHE_KEY, games, GAMES_CACHE_TTL)
    return games


def _invalidate_games_cache() -> None:
    cache.delete(GAMES_CACHE_KEY)


async def _show_games(message: Message):
    """Показывает список ближайших игр."""
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    games = await _get_upcoming_games_cached()
    if not games:
        await message.answer(
            "😔 Пока нет доступных игр.\n\n"
            "Загляни позже — мы добавляем новые игры регулярно!",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer(
        "🎾 <b>Ближайшие игры</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери игру и нажми «Записаться»:",
        parse_mode="HTML",
    )
    for game in games:
        taken = game["taken"]
        free_slots = game["total_slots"] - taken

        text = (
            f"📅 <b>{game['game_date'].strftime('%d.%m.%Y')}</b> в {str(game['game_time'])[:5]}\n"
            f"📍 {game['location']}\n"
            f"💰 {game['price']} ₽\n"
            f"👥 Свободно мест: <b>{free_slots}</b> из {game['total_slots']}"
        )

        if free_slots > 0:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Записаться", callback_data=f"book:{game['id']}")]
            ])
        else:
            text += "\n\n❌ <b>Мест нет</b>"
            keyboard = None

        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


async def _show_my_bookings(message: Message):
    """Показывает активные записи пользователя."""
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
            "📋 У тебя пока нет активных записей.\n\n"
            "Посмотри доступные игры в разделе «🎾 Игры»",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer(
        "📋 <b>Твои записи</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Нажми «Отменить», если не сможешь прийти:",
        parse_mode="HTML",
    )
    for b in bookings:
        status_emoji = "✅" if b['status'] == 'подтверждена' else "⏳"
        text = (
            f"📅 <b>{b['game_date'].strftime('%d.%m.%Y')}</b> в {str(b['game_time'])[:5]}\n"
            f"📍 {b['location']}\n"
            f"📌 Статус: {status_emoji} {b['status']}"
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить запись", callback_data=f"cancel:{b['id']}")]
        ])
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")


def _format_statistics(stats: dict) -> str:
    return (
        "📊 <b>Моя статистика</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📝 Всего заявок подано: <b>{stats['total']}</b>\n"
        f"💳 Игр оплачено: <b>{stats['paid']}</b>\n"
        f"✅ Игр посещено: <b>{stats['attended']}</b>\n"
        f"❌ Игр отменено: <b>{stats['cancelled']}</b>\n\n"
        f"📈 Посещаемость: <b>{stats['attendance_rate']}%</b>\n"
        f"⏱ Сыграно часов: <b>{stats['hours_played']}</b>\n\n"
        "<i>Посещения отмечаются администратором в CRM.</i>"
    )


def _coaches_list_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            text=f"{c['emoji']} {c['name']}",
            callback_data=f"coach:{c['id']}",
        )]
        for c in COACHES
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(F.text == BTN_MAIN_MENU)
async def menu_main(message: Message, state: FSMContext):
    await state.clear()
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer("Сначала заполни анкету: отправь /start")
        return
    await show_main_menu(message, user)


@router.message(F.text == BTN_GAMES)
async def menu_games(message: Message):
    await _show_games(message)


@router.message(F.text == BTN_MY_BOOKINGS)
async def menu_my_bookings(message: Message):
    await _show_my_bookings(message)


@router.message(F.text == BTN_ABOUT_PADEL)
async def menu_about_padel(message: Message):
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    await message.answer(
        PADEL_INFO_TEXT,
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(F.text == BTN_STATS)
async def menu_stats(message: Message):
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
async def menu_coaches(message: Message):
    user = await _require_registered(message.from_user.id)
    if not user:
        await message.answer(
            "❌ Сначала заполни анкету.\n"
            "Отправь команду /start для регистрации.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return
    await message.answer(
        "👨‍🏫 <b>Наши тренеры</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери тренера, чтобы узнать подробнее:\n\n"
        "<i>Скоро тренеров можно будет добавлять через CRM.</i>",
        reply_markup=_coaches_list_keyboard(),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("coach:"))
async def show_coach_detail(callback: CallbackQuery):
    coach_id = int(callback.data.split(":")[1])
    coach = next((c for c in COACHES if c["id"] == coach_id), None)
    if not coach:
        await callback.answer("Тренер не найден", show_alert=True)
        return

    await callback.answer()
    await callback.message.answer(
        f"{coach['emoji']} <b>{coach['name']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📝 {coach['description']}\n\n"
        f"🏆 <b>Достижения:</b>\n{coach['achievements']}\n\n"
        "<i>Запись на тренировку — через администратора 💬</i>",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(F.text == BTN_CONTACT_ADMIN)
async def menu_contact_admin(message: Message, state: FSMContext):
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


@router.message(StateFilter(AdminContact.waiting_for_message))
async def process_admin_message(message: Message, state: FSMContext, bot: Bot):
    if message.text == BTN_MAIN_MENU:
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

    if ADMIN_CHAT_ID:
        admin_text = (
            "💬 <b>Сообщение от игрока</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 {user['name']}\n"
            f"📞 {user['phone']}\n"
            f"🆔 Telegram ID: {message.from_user.id}\n\n"
            f"📝 {user_message}"
        )
        try:
            await bot.send_message(ADMIN_CHAT_ID, admin_text, parse_mode="HTML")
            await message.answer(
                "✅ Сообщение отправлено администратору!\n\n"
                "Ответ придёт в этот чат.",
                reply_markup=main_menu_keyboard(),
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение админу: {e}")
            await message.answer(
                "❌ Не удалось отправить сообщение.\n"
                "Попробуй позже.",
                reply_markup=main_menu_keyboard(),
            )
    else:
        await message.answer(
            "⚠️ Связь с администратором временно недоступна.\n\n"
            "Попробуй позже.",
            reply_markup=main_menu_keyboard(),
        )


# ---------------------------------------------------------------------------
# Список игр и запись
# ---------------------------------------------------------------------------

async def _require_registered(telegram_id: int):
    """Возвращает пользователя из БД или None, если анкета не заполнена."""
    return await db.get_user_by_telegram_id(telegram_id)


async def _notify_admin_new_booking(bot: Bot, user: dict, game: dict, booking_id: int):
    """Отправляет админу уведомление о новой записи игрока на корт."""
    if not ADMIN_CHAT_ID:
        logger.warning("ADMIN_CHAT_ID не задан — уведомление о записи пропущено")
        return

    game_datetime = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
    )
    notification_text = (
        "🔔 <b>Новая запись на корт!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Имя: {user['name']}\n"
        f"📞 Телефон: {user['phone']}\n"
        f"📅 Дата и время: {game_datetime}\n"
        f"📍 Корт / площадка: {game['location']}\n"
        f"💰 Стоимость: {game['price']} ₽\n"
        f"🆔 ID заявки: {booking_id}"
    )
    try:
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=notification_text,
            parse_mode="HTML",
        )
        logger.debug(
            "Уведомление о записи #%s отправлено админу (игрок: %s)",
            booking_id,
            user["name"],
        )
    except Exception as e:
        logger.error("Не удалось отправить уведомление админу о записи #%s: %s", booking_id, e)


@router.message(Command("games"))
async def cmd_games(message: Message):
    await _show_games(message)


@router.callback_query(F.data.startswith("book:"))
async def process_booking(callback: CallbackQuery):
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.answer("Сначала заполни анкету: отправь /start", show_alert=True)
        return

    game_id = int(callback.data.split(":")[1])
    game = await db.get_game_by_id(game_id)

    if not game:
        await callback.answer("Эта игра больше не доступна.", show_alert=True)
        return

    # Проверка мест и вставка заявки выполняются атомарно в одной транзакции
    # с блокировкой строки игры — это исключает race condition, когда два
    # игрока одновременно проходят проверку на последнее свободное место.
    result = await db.create_booking_safe(user_id=user["id"], game_id=game_id)

    if result["status"] == "not_found":
        await callback.answer("Эта игра больше не доступна.", show_alert=True)
        return
    if result["status"] == "full":
        await callback.answer("К сожалению, места уже закончились.", show_alert=True)
        return
    if result["status"] == "duplicate":
        await callback.answer("Ты уже записан на эту игру.", show_alert=True)
        return

    booking = result["booking"]
    booking_id = booking["id"]
    _invalidate_games_cache()

    await _notify_admin_new_booking(callback.bot, user, game, booking_id)

    await callback.answer("Заявка отправлена! ✅")
    await callback.message.answer(
        f"✅ Ты записан на игру {game['game_date'].strftime('%d.%m.%Y')} "
        f"в {str(game['game_time'])[:5]}!\n\n"
        "📌 Статус заявки: <b>новая</b>\n"
        "Администратор подтвердит её после оплаты.\n\n"
        "Посмотреть записи: «📋 Мои записи» в меню",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# Мои записи и отмена
# ---------------------------------------------------------------------------

@router.message(Command("my_bookings"))
async def cmd_my_bookings(message: Message):
    await _show_my_bookings(message)


@router.callback_query(F.data.startswith("cancel:"))
async def process_cancel(callback: CallbackQuery):
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.answer("Сначала заполни анкету: отправь /start", show_alert=True)
        return

    booking_id = int(callback.data.split(":")[1])

    # Отмена проверяет владельца записи внутри транзакции — раньше можно было
    # отменить чужую запись, зная её booking_id (IDOR).
    result = await db.cancel_booking_owned(booking_id, user["id"])

    if result == "not_found":
        await callback.answer("Запись не найдена.", show_alert=True)
        return
    if result == "forbidden":
        await callback.answer("Это не твоя запись.", show_alert=True)
        return

    _invalidate_games_cache()
    await callback.answer("Запись отменена.")
    await callback.message.edit_text(callback.message.text + "\n\n❌ ОТМЕНЕНО")


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
            "ℹ️ Я понимаю команды /start, /games, /my_bookings и /cancel.\n"
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
# Напоминания за 24 часа до игры
# ---------------------------------------------------------------------------

async def send_reminders(bot: Bot):
    """Эта функция запускается автоматически каждый час планировщиком APScheduler."""
    games = await db.get_games_needing_reminder()
    for game in games:
        participants = await db.get_participants_for_game(game["id"])
        for p in participants:
            try:
                await bot.send_message(
                    p["telegram_id"],
                    f"⏰ Напоминание! Завтра у тебя игра в падел:\n"
                    f"📅 {game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}\n"
                    f"📍 {game['location']}\n\n"
                    "До встречи на корте! 🎾"
                )
            except Exception as e:
                logger.error(f"Не удалось отправить напоминание пользователю {p['telegram_id']}: {e}")
        await db.mark_reminder_sent(game["id"])


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN. Проверьте файл .env")

    # Пул соединений asyncpg создаётся один раз на процесс бота, до старта
    # диспетчера — все обработчики дальше просто берут соединения из пула.
    await db.get_pool()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Планировщик проверяет раз в час, кому пора отправить напоминание за 24 часа
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_reminders, "interval", hours=1, args=[bot])
    scheduler.start()

    try:
        # Если задан WEBHOOK_URL - используем вебхуки, иначе long polling
        if WEBHOOK_URL:
            logger.info(f"Запуск бота с вебхуком: {WEBHOOK_URL}{WEBHOOK_PATH}")
            await bot.set_webhook(url=f"{WEBHOOK_URL}{WEBHOOK_PATH}")
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
