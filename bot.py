"""
bot.py
------
Telegram-бот для игроков в падел.

Что умеет:
- /start — расширенная анкета (имя, возраст, город, опыт, инвентарь, правила, телефон)
- /menu — главное меню, /help — написать администратору
- показывает список ближайших игр, спрашивает количество мест (1-4) и
  позволяет записаться с автоматическим расчётом цены
- после записи предлагает оплату (карта / СБП QR — интерфейс рабочий,
  провайдер — заглушка, см. payment_provider.py)
- /my_bookings — список своих записей с возможностью отмены
- автоматически шлёт напоминания за 24 и за 2 часа до игры с кнопкой
  «Не смогу» (отменяет заявку и освобождает место)
- /help пересылает сообщение администратору с кнопкой «Ответить»

Запуск (когда виртуальное окружение активировано):
    python bot.py
"""

import asyncio
import logging
import os
import re
import time
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, Dict, Optional

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
    LabeledPrice,
    Message,
    CallbackQuery,
    PreCheckoutQuery,
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

# Ключи/TTL кэша живут в cache.py — там же их читает app.py (CRM), чтобы
# сбрасывать кэш игр при создании/редактировании игры или заявки. Раньше
# CRM ничего не знала об этом кэше, из-за чего бот мог до 30 сек (или до
# перезапуска процесса, если бот и CRM — разные процессы без Redis)
# показывать устаревший список игр после изменений в CRM.
GAMES_CACHE_KEY = cache.GAMES_CACHE_KEY
GAMES_CACHE_TTL = cache.GAMES_CACHE_TTL
LEVELS_CACHE_KEY = cache.LEVELS_CACHE_KEY
LEVELS_CACHE_TTL = cache.LEVELS_CACHE_TTL

RATE_LIMIT_PER_SECOND = int(os.getenv("BOT_RATE_LIMIT_PER_SECOND", "10"))

# Команды бота (меню "/" в Telegram-клиенте) — регистрируются через
# setMyCommands при старте (см. setup_bot_commands(), вызывается и из
# main() здесь, и из run_bot() в app.py, если бот встроен в CRM-процесс).
BOT_COMMANDS = [
    BotCommand(command="start", description="Начать / открыть анкету"),
    BotCommand(command="menu", description="Главное меню"),
    BotCommand(command="help", description="Написать администратору"),
]


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(BOT_COMMANDS)


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


def _game_card(game: dict) -> tuple[str, Optional[InlineKeyboardMarkup]]:
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

    return text, keyboard


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


async def _show_games(message: Message):
    """Показывает список ближайших игр."""
    t_start = time.monotonic()
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

    # Раньше карточки игр отправлялись последовательно (await в цикле), то
    # есть каждая дополнительная игра добавляла ещё один сетевой round-trip
    # до Telegram (~100-400 мс) к общему времени ответа. Отправляем все
    # карточки конкурентно — суммарное время ≈ время одного round-trip,
    # а не N.
    sends = [_send_answer(message, *_game_card(game)) for game in games]
    results = await asyncio.gather(*sends, return_exceptions=True)
    for game, result in zip(games, results):
        if isinstance(result, Exception):
            logger.error("Не удалось отправить карточку игры #%s: %s", game.get("id"), result)

    logger.debug("_show_games: всего %.3f с, %d игр", time.monotonic() - t_start, len(games))


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

    def _booking_card(b: dict) -> tuple[str, InlineKeyboardMarkup]:
        status_emoji = "✅" if b['status'] == 'подтверждена' else "⏳"
        text = (
            f"📅 <b>{b['game_date'].strftime('%d.%m.%Y')}</b> в {str(b['game_time'])[:5]}\n"
            f"📍 {b['location']}\n"
            f"👥 Мест: {b.get('slots_count', 1)}\n"
            f"📌 Статус: {status_emoji} {b['status']}"
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить запись", callback_data=f"cancel:{b['id']}")]
        ])
        return text, keyboard

    # Как и в _show_games, отправляем карточки конкурентно, а не по одной,
    # оборачивая каждый answer() в настоящую корутину (см. _send_answer).
    sends = [_send_answer(message, *_booking_card(b)) for b in bookings]
    results = await asyncio.gather(*sends, return_exceptions=True)
    for b, result in zip(bookings, results):
        if isinstance(result, Exception):
            logger.error("Не удалось отправить карточку записи #%s: %s", b.get("id"), result)


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
@router.message(Command("menu"))
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
@router.message(Command("help"))
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
            "💬 <b>Сообщение от игрока (/help)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 {user['name']}\n"
            f"📞 {user['phone']}\n"
            f"🆔 Telegram ID: {message.from_user.id}\n\n"
            f"📝 {user_message}"
        )
        # Кнопка «Ответить» — нажатие запускает FSM-диалог AdminReply прямо
        # в чате администратора (см. admin_reply_start/admin_reply_send):
        # он пишет обычный текст, бот сам находит нужного игрока по
        # telegram_id, зашитому в callback_data, и пересылает ответ.
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Ответить", callback_data=f"reply_to:{message.from_user.id}")]
        ])
        try:
            await bot.send_message(
                ADMIN_CHAT_ID, admin_text, parse_mode="HTML", reply_markup=admin_keyboard
            )
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


@router.callback_query(F.data.startswith("reply_to:"))
async def admin_reply_start(callback: CallbackQuery, state: FSMContext):
    """Админ нажал «↩️ Ответить» под сообщением игрока — просим текст
    ответа. Кнопка существует только в сообщениях, отправленных в
    ADMIN_CHAT_ID, поэтому нажать её может только тот, у кого есть доступ к
    этому чату (Telegram не позволяет «нажать» инлайн-кнопку в чужом чате)."""
    target_telegram_id = int(callback.data.split(":", 1)[1])
    await state.update_data(reply_target_telegram_id=target_telegram_id)
    await state.set_state(AdminReply.waiting_for_reply)
    await callback.answer()
    await callback.message.answer(
        "✏️ Напиши ответ игроку — я перешлю его в бот.\n"
        "<i>Для отмены отправь /cancel</i>",
        parse_mode="HTML",
    )


@router.message(StateFilter(AdminReply.waiting_for_reply))
async def admin_reply_send(message: Message, state: FSMContext, bot: Bot):
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
            f"💬 <b>Ответ администратора:</b>\n\n{reply_text}",
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

async def _require_registered(telegram_id: int):
    """Возвращает пользователя из БД или None, если анкета не заполнена."""
    return await db.get_user_by_telegram_id(telegram_id)


async def _notify_admin_new_booking(
    bot: Bot, user: dict, game: dict, booking_id: int, slots_count: int, total_price: float,
):
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
        f"👥 Мест: {slots_count}\n"
        f"💰 К оплате: {total_price:.0f} ₽\n"
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


def _slots_choice_keyboard(game_id: int, max_slots: int) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(text=str(n), callback_data=f"book_slots:{game_id}:{n}")
        for n in range(1, max_slots + 1)
    ]
    return InlineKeyboardMarkup(inline_keyboard=[buttons])


@router.callback_query(F.data.startswith("book:"))
async def process_booking_ask_slots(callback: CallbackQuery):
    """Первый шаг записи: показываем игру и спрашиваем, на сколько мест
    бронировать (1-4), прежде чем создавать заявку."""
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.answer("Сначала заполни анкету: отправь /start", show_alert=True)
        return

    game_id = int(callback.data.split(":")[1])
    game = await db.get_game_by_id(game_id)
    if not game:
        await callback.answer("Эта игра больше не доступна.", show_alert=True)
        return

    taken = await db.count_bookings_for_game(game_id)
    free_slots = game["total_slots"] - taken
    if free_slots <= 0:
        await callback.answer("К сожалению, места уже закончились.", show_alert=True)
        return

    max_choice = min(MAX_SLOTS_PER_BOOKING, free_slots)
    await callback.answer()
    await callback.message.answer(
        "👥 <b>Сколько мест забронировать?</b>\n"
        f"Свободно: {free_slots} из {game['total_slots']}\n"
        f"Цена за место: {game['price']} ₽\n\n"
        "Выбери количество:",
        reply_markup=_slots_choice_keyboard(game_id, max_choice),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("book_slots:"))
async def process_booking_confirm(callback: CallbackQuery):
    """Второй шаг записи: пользователь выбрал количество мест — создаём
    заявку, считаем итоговую цену и предлагаем способ оплаты."""
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.answer("Сначала заполни анкету: отправь /start", show_alert=True)
        return

    _, game_id_str, slots_str = callback.data.split(":")
    game_id = int(game_id_str)
    slots_count = int(slots_str)

    # Проверка мест и вставка заявки выполняются атомарно в одной транзакции
    # с блокировкой строки игры — это исключает race condition, когда два
    # игрока одновременно проходят проверку на последнее свободное место.
    # Игру отдельно заранее не запрашиваем: create_booking_safe уже читает
    # её внутри транзакции и возвращает нам эти же данные — раньше здесь был
    # лишний round-trip к БД на каждую попытку записи.
    result = await db.create_booking_safe(user_id=user["id"], game_id=game_id, slots_count=slots_count)

    if result["status"] == "not_found":
        await callback.answer("Эта игра больше не доступна.", show_alert=True)
        return
    if result["status"] == "full":
        await callback.answer("К сожалению, свободных мест уже меньше, чем ты выбрал.", show_alert=True)
        return
    if result["status"] == "duplicate":
        await callback.answer("Ты уже записан на эту игру.", show_alert=True)
        return

    booking = result["booking"]
    game = result["game"]
    booking_id = booking["id"]
    total_price = float(game["price"]) * slots_count
    _invalidate_games_cache()

    # Заявка создаётся сразу со «своим» платежом (статус «ожидает», способ
    # оплаты пока не выбран) — так администратор в CRM видит ожидаемую
    # оплату даже если игрок не пройдёт шаги ниже до конца (например,
    # оплатит наличными на месте).
    payment = await db.create_payment(booking_id, total_price)

    # Ответ пользователю не должен ждать отправки уведомления админу —
    # раньше это был await ДО ответа пользователю, то есть каждая запись на
    # игру платила ещё одним полным сетевым round-trip до Telegram сверху.
    # Запускаем как fire-and-forget задачу; ошибки уже логируются внутри
    # _notify_admin_new_booking и не могут сломать ответ игроку.
    asyncio.create_task(
        _notify_admin_new_booking(callback.bot, user, game, booking_id, slots_count, total_price)
    )

    await callback.answer("Заявка отправлена! ✅")
    await callback.message.answer(
        f"✅ Ты записан на игру {game['game_date'].strftime('%d.%m.%Y')} "
        f"в {str(game['game_time'])[:5]}!\n\n"
        f"👥 Мест: <b>{slots_count}</b>\n"
        f"💰 К оплате: <b>{total_price:.0f} ₽</b>\n"
        "📌 Статус заявки: <b>новая</b>\n\n"
        "Посмотреть записи: «📋 Мои записи» в меню",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML",
    )
    await callback.message.answer(
        _payment_prompt_text(total_price),
        reply_markup=_payment_method_keyboard(payment["id"]),
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# Оплата (интерфейс — рабочий; провайдер — заглушка, см. payment_provider.py)
# ---------------------------------------------------------------------------

def _payment_prompt_text(amount: float) -> str:
    return (
        "💳 <b>Оплата</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Сумма к оплате: <b>{amount:.0f} ₽</b>\n\n"
        "Выбери способ оплаты:"
    )


def _payment_method_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить картой", callback_data=f"pay_card:{payment_id}")],
        [InlineKeyboardButton(text="📱 Оплатить по СБП (QR)", callback_data=f"pay_sbp:{payment_id}")],
    ])


def _paid_notify_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"paid_notify:{payment_id}")]
    ])


@router.callback_query(F.data.startswith("pay_card:"))
async def process_pay_card(callback: CallbackQuery):
    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_by_id(payment_id)
    if not payment:
        await callback.answer("Платёж не найден.", show_alert=True)
        return

    await db.set_payment_method(payment_id, "card")
    await callback.answer()

    if payment_provider.is_card_provider_configured():
        # Настоящий Telegram Payments API — сработает, если в .env указан
        # реальный (или тестовый) PAYMENT_PROVIDER_TOKEN, подключённый через
        # @BotFather. Подтверждение оплаты — в process_successful_payment.
        await callback.message.answer_invoice(
            title="Оплата игры в падел",
            description=f"Бронирование #{payment['booking_id']} на {float(payment['amount']):.0f} ₽",
            payload=f"payment:{payment_id}",
            provider_token=os.getenv("PAYMENT_PROVIDER_TOKEN"),
            currency="RUB",
            prices=[LabeledPrice(label="Игра в падел", amount=int(round(float(payment["amount"]) * 100)))],
        )
        return

    reference = payment_provider.generate_stub_reference("CARD", payment_id)
    await callback.message.answer(
        "💳 <b>Оплата картой (демо-режим)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Платёжный провайдер пока не подключён — это временная заглушка "
        "интерфейса оплаты (см. PAYMENT_PROVIDER_TOKEN в .env).\n\n"
        f"Сумма: <b>{float(payment['amount']):.0f} ₽</b>\n"
        f"Референс операции: <code>{reference}</code>\n\n"
        "Оплати администратору на месте/переводом и нажми кнопку ниже:",
        reply_markup=_paid_notify_keyboard(payment_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("pay_sbp:"))
async def process_pay_sbp(callback: CallbackQuery):
    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_by_id(payment_id)
    if not payment:
        await callback.answer("Платёж не найден.", show_alert=True)
        return

    await db.set_payment_method(payment_id, "sbp")
    await callback.answer()

    # СБП не входит в стандартный Telegram Payments API — реального
    # приёма нет, показываем интерфейс (QR с тестовыми данными) и просим
    # игрока подтвердить оплату самостоятельно, как и при оплате картой без
    # подключённого провайдера.
    reference = payment_provider.generate_stub_reference("SBP", payment_id)
    qr_payload = payment_provider.build_sbp_payload(float(payment["amount"]), reference)
    qr_bytes = payment_provider.make_qr_image_bytes(qr_payload)

    await callback.message.answer_photo(
        BufferedInputFile(qr_bytes, filename="sbp_qr.png"),
        caption=(
            "📱 <b>Оплата по СБП (демо-режим)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Реальный приём платежей по СБП пока не подключён — это заглушка "
            "интерфейса (сгенерирован тестовый QR).\n\n"
            f"Сумма: <b>{float(payment['amount']):.0f} ₽</b>\n"
            f"Референс операции: <code>{reference}</code>\n\n"
            "Оплати администратору на месте/переводом и нажми кнопку ниже:"
        ),
        reply_markup=_paid_notify_keyboard(payment_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("paid_notify:"))
async def process_paid_notify(callback: CallbackQuery):
    """Игрок сообщает, что оплатил (наличными/переводом/по заглушке QR) —
    окончательное подтверждение всё равно делает администратор в CRM,
    чтобы нельзя было просто нажать кнопку и получить статус «оплачено»
    без реальной проверки оплаты."""
    payment_id = int(callback.data.split(":")[1])
    payment = await db.get_payment_by_id(payment_id)
    if not payment:
        await callback.answer("Платёж не найден.", show_alert=True)
        return

    await callback.answer("Спасибо! Администратор проверит оплату.", show_alert=True)
    await callback.message.answer(
        "⏳ Мы получили твоё уведомление об оплате.\n"
        "Администратор подтвердит её в ближайшее время."
    )

    if ADMIN_CHAT_ID:
        try:
            # Имя берём из профиля в БД (то, что игрок указал при регистрации
            # в боте, — надёжнее, чем Telegram-имя, которое человек может
            # менять как угодно); username — только из Telegram, в профиле
            # его не храним, и брать его больше не откуда.
            db_user = await db.get_user_by_telegram_id(callback.from_user.id)
            display_name = (db_user["name"] if db_user else None) or callback.from_user.full_name or "Игрок"
            username = callback.from_user.username
            username_part = f" (@{username})" if username else ""

            await callback.bot.send_message(
                ADMIN_CHAT_ID,
                "💰 <b>Игрок сообщил об оплате</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Пользователь {display_name}{username_part} сообщил об оплате "
                f"брони №{payment['booking_id']}\n\n"
                f"🆔 Платёж #{payment_id}\n"
                f"💳 Способ: {payment.get('method') or '—'}\n"
                f"💰 Сумма: {payment['amount']} ₽\n\n"
                "Проверь и подтверди оплату в CRM (раздел «Оплаты»).",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Не удалось уведомить админа об оплате #%s: %s", payment_id, e)


@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    """Telegram требует ответить в течение 10 секунд, иначе платёж
    отклоняется — здесь можно было бы ещё раз проверить наличие мест, но
    места уже были зарезервированы на шаге создания заявки, поэтому просто
    подтверждаем."""
    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def process_successful_payment(message: Message):
    """Срабатывает только при настоящей интеграции Telegram Payments
    (PAYMENT_PROVIDER_TOKEN задан) — Telegram сам подтверждает, что деньги
    списаны, поэтому здесь можно сразу помечать оплату подтверждённой без
    участия администратора."""
    payload = message.successful_payment.invoice_payload
    try:
        payment_id = int(payload.split(":", 1)[1])
    except (IndexError, ValueError):
        logger.error("Некорректный payload успешного платежа: %s", payload)
        return

    await db.confirm_payment(payment_id)
    await message.answer("✅ Оплата картой прошла успешно! Спасибо 🎾")

    if ADMIN_CHAT_ID:
        try:
            await message.bot.send_message(
                ADMIN_CHAT_ID,
                f"💳 Оплата #{payment_id} подтверждена автоматически (Telegram Payments).",
            )
        except Exception as e:
            logger.error("Не удалось уведомить админа об автооплате #%s: %s", payment_id, e)


# ---------------------------------------------------------------------------
# Мои записи и отмена
# ---------------------------------------------------------------------------

@router.message(Command("my_bookings"))
async def cmd_my_bookings(message: Message):
    await _show_my_bookings(message)


async def _notify_refund(callback: CallbackQuery, user: dict, booking_id: int, game: Optional[dict], payment: dict) -> None:
    """Уведомляет игрока и админа о возврате оплаты (>24ч до игры, была
    подтверждённая оплата) и пишет об этом в общий журнал admin_logs —
    вызывается после успешной отмены брони, поэтому ошибки здесь не должны
    влиять на уже случившуюся отмену, только логируются."""
    await callback.message.answer(
        "Ваша запись отменена. Оплата будет возвращена на ваш счёт в ближайшее время."
    )

    game_dt = (
        f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
        if game else "—"
    )
    location = game["location"] if game else "—"
    amount = float(payment["amount"])
    username = callback.from_user.username
    username_part = f" (@{username})" if username else ""

    if ADMIN_CHAT_ID:
        try:
            await callback.bot.send_message(
                ADMIN_CHAT_ID,
                "❌ <b>Отмена записи с возвратом оплаты</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Пользователь {user['name']}{username_part} отменил запись "
                f"на корт (бронь №{booking_id}) более чем за 24 часа до начала\n\n"
                f"📅 {game_dt}\n"
                f"📍 {location}\n"
                f"💰 К возврату: {amount:.0f} ₽\n\n"
                "Статус оплаты изменён на «возврат» — оформите возврат средств игроку.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Не удалось уведомить админа об отмене брони №%s: %s", booking_id, e)

    description = (
        f"Возврат оплаты {amount:.0f} ₽ по брони №{booking_id}: пользователь "
        f"{user['name']}{username_part} отменил запись на игру {game_dt} "
        "более чем за 24 часа до начала"
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


@router.callback_query(F.data.startswith("cancel:"))
async def process_cancel(callback: CallbackQuery):
    user = await _require_registered(callback.from_user.id)
    if not user:
        await callback.answer("Сначала заполни анкету: отправь /start", show_alert=True)
        return

    booking_id = int(callback.data.split(":")[1])

    # Отмена проверяет владельца записи внутри транзакции — раньше можно было
    # отменить чужую запись, зная её booking_id (IDOR). Дополнительно решает,
    # положен ли возврат оплаты (>24ч до игры + была подтверждённая оплата).
    result = await db.cancel_booking_owned(booking_id, user["id"])

    if result["status"] == "not_found":
        await callback.answer("Запись не найдена.", show_alert=True)
        return
    if result["status"] == "forbidden":
        await callback.answer("Это не твоя запись.", show_alert=True)
        return

    _invalidate_games_cache()
    await callback.answer("Запись отменена.")
    await callback.message.edit_text(callback.message.text + "\n\n❌ ОТМЕНЕНО")

    if result.get("refund_eligible") and result.get("payment"):
        await _notify_refund(callback, user, booking_id, result.get("game"), result["payment"])


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
            "ℹ️ Я понимаю команды /start, /menu, /games, /my_bookings, /help и /cancel.\n"
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

async def _send_reminder_batch(bot: Bot, games: list, label: str, mark_sent) -> None:
    """Рассылает напоминание с кнопкой «Не смогу» участникам списка игр и
    помечает игру как обработанную. Кнопка использует существующий callback
    cancel:<booking_id> (см. process_cancel) — отдельный обработчик не
    нужен, отмена брони работает одинаково откуда угодно."""
    for game in games:
        participants = await db.get_participants_for_game(game["id"])
        game_dt = f"{game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}"
        text = (
            f"⏰ <b>Напоминание!</b> {label} у тебя игра в падел:\n"
            f"📅 {game_dt}\n"
            f"📍 {game['location']}\n\n"
            "Если не сможешь прийти — нажми «Не смогу», место освободится "
            "для других игроков.\n\n"
            "До встречи на корте! 🎾"
        )

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
        for p, result in zip(participants, results):
            if isinstance(result, Exception):
                logger.error(
                    "Не удалось отправить напоминание пользователю %s: %s",
                    p["telegram_id"], result,
                )
        await mark_sent(game["id"])


async def send_reminders(bot: Bot):
    """Запускается планировщиком (см. main()/app.py:run_bot) каждые 15
    минут: проверяет, кому пора напомнить за 24 и за 2 часа до игры."""
    games_24h = await db.get_games_needing_reminder_24h()
    await _send_reminder_batch(bot, games_24h, "Через 24 часа", db.mark_reminder_24h_sent)

    games_2h = await db.get_games_needing_reminder_2h()
    await _send_reminder_batch(bot, games_2h, "Через 2 часа", db.mark_reminder_2h_sent)


def _make_reminder_job(bot: Bot, loop: asyncio.AbstractEventLoop):
    """APScheduler (BackgroundScheduler) работает в СВОЁМ отдельном потоке
    и не умеет напрямую вызывать async-функции — job должен быть обычной
    (синхронной) функцией. Передаём корутину в event loop бота через
    run_coroutine_threadsafe и ждём результат с таймаутом: так планировщик
    не блокирует ни свой поток, ни event loop бота, а БД/сеть по-прежнему
    работают через тот же asyncpg-пул, что и остальные обработчики."""

    def _job() -> None:
        future = asyncio.run_coroutine_threadsafe(send_reminders(bot), loop)
        try:
            future.result(timeout=60)
        except Exception as e:
            logger.error("Ошибка задачи напоминаний: %s", e)

    return _job


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
    scheduler = BackgroundScheduler()
    scheduler.add_job(_make_reminder_job(bot, loop), "interval", minutes=15)
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
