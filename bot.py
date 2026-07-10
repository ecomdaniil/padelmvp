"""
bot.py
------
Telegram-бот для игроков в падел.

Что умеет:
- /start — знакомство и заполнение анкеты (имя, телефон, уровень игры)
- показывает список ближайших игр и позволяет записаться
- /my_bookings — список своих записей с возможностью отмены
- автоматически шлёт напоминание за 24 часа до игры

Запуск (когда виртуальное окружение активировано):
    python bot.py
"""

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, Router
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
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

import database as db

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")

router = Router()


# ---------------------------------------------------------------------------
# FSM-состояния для заполнения анкеты
# ---------------------------------------------------------------------------

class RegistrationForm(StatesGroup):
    waiting_for_name = State()
    waiting_for_phone = State()
    waiting_for_level = State()


LEVEL_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Начинающий")],
        [KeyboardButton(text="Средний")],
        [KeyboardButton(text="Продвинутый")],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)

CONTACT_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Отправить номер телефона", request_contact=True)]],
    resize_keyboard=True,
    one_time_keyboard=True,
)


# ---------------------------------------------------------------------------
# /start и заполнение анкеты
# ---------------------------------------------------------------------------

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    existing_user = db.get_user_by_telegram_id(message.from_user.id)

    if existing_user:
        await message.answer(
            f"С возвращением, {existing_user['name']}! 🎾\n\n"
            "Доступные команды:\n"
            "/games — посмотреть игры и записаться\n"
            "/my_bookings — мои записи (можно отменить)\n"
        )
        return

    await state.set_state(RegistrationForm.waiting_for_name)
    await message.answer(
        "Привет! 🎾 Это бот для записи на игры в падел.\n\n"
        "Для начала давай заполним твою анкету.\n"
        "Как тебя зовут?",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(StateFilter(RegistrationForm.waiting_for_name))
async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(RegistrationForm.waiting_for_phone)
    await message.answer(
        "Отлично! Теперь укажи свой номер телефона для связи "
        "(можно нажать кнопку ниже или написать вручную):",
        reply_markup=CONTACT_KEYBOARD,
    )


@router.message(StateFilter(RegistrationForm.waiting_for_phone))
async def process_phone(message: Message, state: FSMContext):
    if message.contact:
        phone = message.contact.phone_number
    else:
        phone = message.text.strip()

    await state.update_data(phone=phone)
    await state.set_state(RegistrationForm.waiting_for_level)
    await message.answer(
        "Какой у тебя уровень игры?",
        reply_markup=LEVEL_KEYBOARD,
    )


@router.message(StateFilter(RegistrationForm.waiting_for_level))
async def process_level(message: Message, state: FSMContext):
    level = message.text.strip()
    data = await state.get_data()

    db.create_user(
        telegram_id=message.from_user.id,
        name=data["name"],
        phone=data["phone"],
        level=level,
    )
    await state.clear()

    await message.answer(
        f"Спасибо, анкета заполнена! ✅\n\n"
        f"Имя: {data['name']}\nТелефон: {data['phone']}\nУровень: {level}\n\n"
        "Теперь можешь посмотреть игры командой /games",
        reply_markup=ReplyKeyboardRemove(),
    )


# ---------------------------------------------------------------------------
# Список игр и запись
# ---------------------------------------------------------------------------

def _require_registered(telegram_id: int):
    """Возвращает пользователя из БД или None, если анкета не заполнена."""
    return db.get_user_by_telegram_id(telegram_id)


@router.message(Command("games"))
async def cmd_games(message: Message):
    user = _require_registered(message.from_user.id)
    if not user:
        await message.answer("Сначала заполни анкету: отправь /start")
        return

    games = db.get_upcoming_games()
    if not games:
        await message.answer("Пока нет доступных игр. Загляни позже!")
        return

    await message.answer("Ближайшие игры:")
    for game in games:
        taken = db.count_bookings_for_game(game["id"])
        free_slots = game["total_slots"] - taken

        text = (
            f"📅 {game['game_date'].strftime('%d.%m.%Y')} в {str(game['game_time'])[:5]}\n"
            f"📍 {game['location']}\n"
            f"💰 {game['price']} ₽\n"
            f"👥 Свободно мест: {free_slots} из {game['total_slots']}"
        )

        if free_slots > 0:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Записаться ✅", callback_data=f"book:{game['id']}")]
            ])
        else:
            text += "\n\n❌ Мест нет"
            keyboard = None

        await message.answer(text, reply_markup=keyboard)


@router.callback_query(F.data.startswith("book:"))
async def process_booking(callback: CallbackQuery):
    user = _require_registered(callback.from_user.id)
    if not user:
        await callback.answer("Сначала заполни анкету: отправь /start", show_alert=True)
        return

    game_id = int(callback.data.split(":")[1])
    game = db.get_game_by_id(game_id)

    if not game:
        await callback.answer("Эта игра больше не доступна.", show_alert=True)
        return

    taken = db.count_bookings_for_game(game_id)
    if taken >= game["total_slots"]:
        await callback.answer("К сожалению, места уже закончились.", show_alert=True)
        return

    db.create_booking(user_id=user["id"], game_id=game_id)

    await callback.answer("Заявка отправлена! ✅")
    await callback.message.answer(
        f"Ты записан на игру {game['game_date'].strftime('%d.%m.%Y')} "
        f"в {str(game['game_time'])[:5]}!\n"
        "Статус заявки: новая. Администратор подтвердит её после оплаты.\n\n"
        "Посмотреть свои записи: /my_bookings"
    )


# ---------------------------------------------------------------------------
# Мои записи и отмена
# ---------------------------------------------------------------------------

@router.message(Command("my_bookings"))
async def cmd_my_bookings(message: Message):
    user = _require_registered(message.from_user.id)
    if not user:
        await message.answer("Сначала заполни анкету: отправь /start")
        return

    bookings = db.get_active_bookings_for_user(user["id"])
    if not bookings:
        await message.answer("У тебя пока нет активных записей. Посмотри игры: /games")
        return

    await message.answer("Твои записи:")
    for b in bookings:
        text = (
            f"📅 {b['game_date'].strftime('%d.%m.%Y')} в {str(b['game_time'])[:5]}\n"
            f"📍 {b['location']}\n"
            f"Статус: {b['status']}"
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отменить запись ❌", callback_data=f"cancel:{b['id']}")]
        ])
        await message.answer(text, reply_markup=keyboard)


@router.callback_query(F.data.startswith("cancel:"))
async def process_cancel(callback: CallbackQuery):
    booking_id = int(callback.data.split(":")[1])
    booking = db.get_booking_by_id(booking_id)

    if not booking:
        await callback.answer("Запись не найдена.", show_alert=True)
        return

    db.cancel_booking(booking_id)
    await callback.answer("Запись отменена.")
    await callback.message.edit_text(callback.message.text + "\n\n❌ ОТМЕНЕНО")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """Если пользователь застрял в заполнении анкеты — можно сбросить командой /cancel.
    Для отмены УЖЕ созданной записи используйте /my_bookings."""
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer("Заполнение анкеты отменено. Отправь /start, чтобы начать заново.")
    else:
        await message.answer("Чтобы отменить запись на игру, используй команду /my_bookings")


# ---------------------------------------------------------------------------
# Напоминания за 24 часа до игры
# ---------------------------------------------------------------------------

async def send_reminders(bot: Bot):
    """Эта функция запускается автоматически каждый час планировщиком APScheduler."""
    games = db.get_games_needing_reminder()
    for game in games:
        participants = db.get_participants_for_game(game["id"])
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
                logger.warning(f"Не удалось отправить напоминание пользователю {p['telegram_id']}: {e}")
        db.mark_reminder_sent(game["id"])


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN. Проверьте файл .env")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Планировщик проверяет раз в час, кому пора отправить напоминание за 24 часа
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_reminders, "interval", hours=1, args=[bot])
    scheduler.start()

    logger.info("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
