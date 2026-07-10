"""
database.py
-----------
Здесь находятся ВСЕ функции для работы с базой данных PostgreSQL.
И бот (bot.py), и CRM (app.py) используют этот файл, чтобы не дублировать код.

Важно про безопасность: мы везде используем параметризованные запросы —
то есть вместо f"SELECT * FROM users WHERE id={id}" (ОПАСНО, SQL-инъекция!)
мы пишем "SELECT * FROM users WHERE id = %s" и передаём параметры отдельно.
Библиотека psycopg2 сама безопасно подставляет значения.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    """Открывает новое соединение с базой данных.
    RealDictCursor — чтобы результаты запросов приходили в виде словарей
    (например row["name"] вместо row[0]), это удобнее для новичка."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# ---------------------------------------------------------------------------
# СОЗДАНИЕ ТАБЛИЦ
# ---------------------------------------------------------------------------

def init_db():
    """Создаёт все таблицы, если их ещё нет. Запускается один раз через init_db.py"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            level TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
            game_date DATE NOT NULL,
            game_time TIME NOT NULL,
            location TEXT NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            total_slots INTEGER NOT NULL,
            reminder_sent BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            game_id INTEGER NOT NULL REFERENCES games(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'новая',
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            booking_id INTEGER NOT NULL REFERENCES bookings(id) ON DELETE CASCADE,
            amount NUMERIC(10, 2) NOT NULL,
            status TEXT NOT NULL DEFAULT 'ожидает',
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            game_id INTEGER REFERENCES games(id) ON DELETE SET NULL,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
            text TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# USERS — анкеты игроков
# ---------------------------------------------------------------------------

def get_user_by_telegram_id(telegram_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user


def create_user(telegram_id: int, name: str, phone: str, level: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO users (telegram_id, name, phone, level)
           VALUES (%s, %s, %s, %s) RETURNING *""",
        (telegram_id, name, phone, level),
    )
    user = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return user


# ---------------------------------------------------------------------------
# GAMES — игры
# ---------------------------------------------------------------------------

def get_upcoming_games():
    """Игры, которые ещё не прошли, отсортированные по дате."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM games
        WHERE (game_date + game_time) >= NOW()
        ORDER BY game_date, game_time
    """)
    games = cur.fetchall()
    cur.close()
    conn.close()
    return games


def get_all_games():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM games ORDER BY game_date DESC, game_time DESC")
    games = cur.fetchall()
    cur.close()
    conn.close()
    return games


def get_game_by_id(game_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM games WHERE id = %s", (game_id,))
    game = cur.fetchone()
    cur.close()
    conn.close()
    return game


def create_game(game_date, game_time, location, price, total_slots):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO games (game_date, game_time, location, price, total_slots)
           VALUES (%s, %s, %s, %s, %s) RETURNING *""",
        (game_date, game_time, location, price, total_slots),
    )
    game = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return game


def update_game(game_id, game_date, game_time, location, price, total_slots):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE games
           SET game_date = %s, game_time = %s, location = %s,
               price = %s, total_slots = %s
           WHERE id = %s""",
        (game_date, game_time, location, price, total_slots, game_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def count_bookings_for_game(game_id: int) -> int:
    """Сколько активных (не отменённых) заявок на игру."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM bookings WHERE game_id = %s AND status != 'отменена'",
        (game_id,),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result["cnt"]


def get_games_needing_reminder():
    """Игры, которые начнутся через 23-25 часов и по которым ещё не отправлено напоминание."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM games
        WHERE reminder_sent = FALSE
          AND (game_date + game_time) BETWEEN NOW() + INTERVAL '23 hours'
                                            AND NOW() + INTERVAL '25 hours'
    """)
    games = cur.fetchall()
    cur.close()
    conn.close()
    return games


def mark_reminder_sent(game_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE games SET reminder_sent = TRUE WHERE id = %s", (game_id,))
    conn.commit()
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# BOOKINGS — заявки на игры
# ---------------------------------------------------------------------------

def create_booking(user_id: int, game_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO bookings (user_id, game_id, status)
           VALUES (%s, %s, 'новая') RETURNING *""",
        (user_id, game_id),
    )
    booking = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return booking


def get_active_bookings_for_user(user_id: int):
    """Заявки пользователя вместе с данными игры, кроме уже отменённых."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT b.*, g.game_date, g.game_time, g.location, g.price
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        WHERE b.user_id = %s AND b.status != 'отменена'
        ORDER BY g.game_date, g.game_time
    """, (user_id,))
    bookings = cur.fetchall()
    cur.close()
    conn.close()
    return bookings


def get_booking_by_id(booking_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM bookings WHERE id = %s", (booking_id,))
    booking = cur.fetchone()
    cur.close()
    conn.close()
    return booking


def cancel_booking(booking_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE bookings SET status = 'отменена' WHERE id = %s", (booking_id,))
    conn.commit()
    cur.close()
    conn.close()


def update_booking_status(booking_id: int, status: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE bookings SET status = %s WHERE id = %s", (status, booking_id))
    conn.commit()
    cur.close()
    conn.close()


def get_all_bookings():
    """Все заявки вместе с именем игрока и данными игры — для админки."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        ORDER BY b.created_at DESC
    """)
    bookings = cur.fetchall()
    cur.close()
    conn.close()
    return bookings


def get_participants_for_game(game_id: int):
    """Список игроков, записанных на конкретную игру (для напоминаний)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.telegram_id, u.name, b.id AS booking_id
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        WHERE b.game_id = %s AND b.status != 'отменена'
    """, (game_id,))
    participants = cur.fetchall()
    cur.close()
    conn.close()
    return participants


# ---------------------------------------------------------------------------
# PAYMENTS — оплаты
# ---------------------------------------------------------------------------

def create_payment(booking_id: int, amount):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO payments (booking_id, amount, status)
           VALUES (%s, %s, 'ожидает') RETURNING *""",
        (booking_id, amount),
    )
    payment = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return payment


def get_all_payments():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*, u.name AS user_name, g.game_date, g.game_time, g.location
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        ORDER BY p.created_at DESC
    """)
    payments = cur.fetchall()
    cur.close()
    conn.close()
    return payments


def confirm_payment(payment_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE payments SET status = 'подтверждена' WHERE id = %s", (payment_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_confirmed_payments_sum_for_game(game_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(p.amount), 0) AS total
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        WHERE b.game_id = %s AND p.status = 'подтверждена'
    """, (game_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result["total"]


# ---------------------------------------------------------------------------
# REVIEWS — отзывы
# ---------------------------------------------------------------------------

def create_review(user_id: int, game_id, rating: int, text: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO reviews (user_id, game_id, rating, text)
           VALUES (%s, %s, %s, %s) RETURNING *""",
        (user_id, game_id, rating, text),
    )
    review = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return review


def get_all_reviews():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, u.name AS user_name
        FROM reviews r
        JOIN users u ON u.id = r.user_id
        ORDER BY r.created_at DESC
    """)
    reviews = cur.fetchall()
    cur.close()
    conn.close()
    return reviews
