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


def _paginated_result(items, total: int, page: int, per_page: int) -> dict:
    """Единый формат ответа для всех *_paginated функций CRM."""
    total_pages = max(1, (total + per_page - 1) // per_page)
    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


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
            age INTEGER,
            city TEXT,
            has_inventory BOOLEAN,
            needs_rules BOOLEAN,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    _migrate_users_table(cur)

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clubs (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            phone TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS action_logs (
            id SERIAL PRIMARY KEY,
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id INTEGER,
            details TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS club_info (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL DEFAULT 'Padel Club',
            description TEXT,
            contact_phone TEXT,
            contact_email TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    # Создаём запись о клубе, если её нет
    cur.execute("""
        INSERT INTO club_info (name, description, contact_phone, contact_email)
        SELECT 'Padel Club', 'Добро пожаловать в наш клуб падел!', '', ''
        WHERE NOT EXISTS (SELECT 1 FROM club_info);
    """)

    conn.commit()
    cur.close()
    conn.close()


def _migrate_users_table(cur):
    """Добавляет новые поля анкеты в существующую таблицу users."""
    migrations = [
        ("age", "INTEGER"),
        ("city", "TEXT"),
        ("has_inventory", "BOOLEAN"),
        ("needs_rules", "BOOLEAN"),
    ]
    for column, col_type in migrations:
        cur.execute(
            f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {column} {col_type};"
        )


def _create_indexes(cur):
    """Создаёт индексы для самых частых и тяжёлых запросов.
    CREATE INDEX IF NOT EXISTS — безопасно вызывать повторно."""
    index_statements = [
        # games: список ближайших игр сортируется/фильтруется по дате и времени
        "CREATE INDEX IF NOT EXISTS idx_games_date_time ON games (game_date, game_time);",
        "CREATE INDEX IF NOT EXISTS idx_games_reminder_pending ON games (reminder_sent) WHERE reminder_sent = FALSE;",

        # bookings: почти все запросы фильтруют по user_id, game_id и/или status
        "CREATE INDEX IF NOT EXISTS idx_bookings_user_id ON bookings (user_id);",
        "CREATE INDEX IF NOT EXISTS idx_bookings_game_id ON bookings (game_id);",
        "CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings (status);",
        "CREATE INDEX IF NOT EXISTS idx_bookings_game_status ON bookings (game_id, status);",
        "CREATE INDEX IF NOT EXISTS idx_bookings_user_status ON bookings (user_id, status);",
        "CREATE INDEX IF NOT EXISTS idx_bookings_created_at ON bookings (created_at DESC);",

        # payments: JOIN по booking_id и фильтрация по статусу оплаты
        "CREATE INDEX IF NOT EXISTS idx_payments_booking_id ON payments (booking_id);",
        "CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (status);",

        # action_logs: журнал всегда читается с ORDER BY created_at DESC
        "CREATE INDEX IF NOT EXISTS idx_action_logs_created_at ON action_logs (created_at DESC);",

        # reviews: список отзывов сортируется по дате
        "CREATE INDEX IF NOT EXISTS idx_reviews_created_at ON reviews (created_at DESC);",
    ]
    for statement in index_statements:
        cur.execute(statement)


def migrate_db():
    """Запускает миграции без пересоздания таблиц. Безопасно вызывать повторно."""
    conn = get_connection()
    cur = conn.cursor()
    _migrate_users_table(cur)
    _create_indexes(cur)
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


def create_user(
    telegram_id: int,
    name: str,
    phone: str,
    level: str,
    age: int = None,
    city: str = None,
    has_inventory: bool = None,
    needs_rules: bool = None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO users
           (telegram_id, name, phone, level, age, city, has_inventory, needs_rules)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (telegram_id, name, phone, level, age, city, has_inventory, needs_rules),
    )
    user = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return user


def update_user(
    telegram_id: int,
    name: str,
    phone: str,
    level: str,
    age: int = None,
    city: str = None,
    has_inventory: bool = None,
    needs_rules: bool = None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE users
           SET name = %s, phone = %s, level = %s,
               age = %s, city = %s, has_inventory = %s, needs_rules = %s
           WHERE telegram_id = %s RETURNING *""",
        (name, phone, level, age, city, has_inventory, needs_rules, telegram_id),
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


def get_games_paginated(page: int = 1, per_page: int = 20):
    """Список игр с пагинацией + количество занятых мест/собранных оплат
    одним запросом (без N+1). Возвращает dict с items/total/total_pages."""
    page = max(1, page)
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM games")
    total = cur.fetchone()["cnt"]

    cur.execute(
        """
        SELECT g.*,
               COALESCE(bk.taken, 0) AS taken,
               COALESCE(pm.collected, 0) AS collected
        FROM games g
        LEFT JOIN (
            SELECT game_id, COUNT(*) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        LEFT JOIN (
            SELECT b.game_id, SUM(p.amount) AS collected
            FROM payments p
            JOIN bookings b ON b.id = p.booking_id
            WHERE p.status = 'подтверждена'
            GROUP BY b.game_id
        ) pm ON pm.game_id = g.id
        ORDER BY g.game_date DESC, g.game_time DESC
        LIMIT %s OFFSET %s
        """,
        (per_page, offset),
    )
    games = cur.fetchall()
    cur.close()
    conn.close()

    return _paginated_result(games, total, page, per_page)


def get_all_games_with_stats():
    """Все игры с занятыми местами/собранными оплатами одним запросом.
    Используется для Excel-отчёта, чтобы не делать 2 запроса на каждую игру."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT g.*,
               COALESCE(bk.taken, 0) AS taken,
               COALESCE(pm.collected, 0) AS collected
        FROM games g
        LEFT JOIN (
            SELECT game_id, COUNT(*) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        LEFT JOIN (
            SELECT b.game_id, SUM(p.amount) AS collected
            FROM payments p
            JOIN bookings b ON b.id = p.booking_id
            WHERE p.status = 'подтверждена'
            GROUP BY b.game_id
        ) pm ON pm.game_id = g.id
        ORDER BY g.game_date DESC, g.game_time DESC
        """
    )
    games = cur.fetchall()
    cur.close()
    conn.close()
    return games


def count_games() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM games")
    total = cur.fetchone()["cnt"]
    cur.close()
    conn.close()
    return total


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


def get_bookings_filtered(search: str = "", status: str = ""):
    """Заявки с фильтрацией по поиску и статусу."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE 1=1
    """
    params = []

    if search:
        query += " AND (u.name ILIKE %s OR u.phone ILIKE %s)"
        params.extend([f"%{search}%", f"%{search}%"])

    if status:
        query += " AND b.status = %s"
        params.append(status)

    query += " ORDER BY b.created_at DESC"

    cur.execute(query, params)
    bookings = cur.fetchall()
    cur.close()
    conn.close()
    return bookings


def get_bookings_paginated(search: str = "", status: str = "", page: int = 1, per_page: int = 20):
    """Заявки с фильтрацией и пагинацией — для CRM, чтобы не грузить всю таблицу целиком."""
    page = max(1, page)
    offset = (page - 1) * per_page

    where_clause = "WHERE 1=1"
    params = []

    if search:
        where_clause += " AND (u.name ILIKE %s OR u.phone ILIKE %s)"
        params.extend([f"%{search}%", f"%{search}%"])

    if status:
        where_clause += " AND b.status = %s"
        params.append(status)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        {where_clause}
        """,
        params,
    )
    total = cur.fetchone()["cnt"]

    cur.execute(
        f"""
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        {where_clause}
        ORDER BY b.created_at DESC
        LIMIT %s OFFSET %s
        """,
        params + [per_page, offset],
    )
    bookings = cur.fetchall()
    cur.close()
    conn.close()

    return _paginated_result(bookings, total, page, per_page)


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


def get_user_statistics(user_id: int) -> dict:
    """Персональная статистика игрока для раздела «Моя статистика»."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM bookings WHERE user_id = %s", (user_id,))
    total = cur.fetchone()["cnt"]

    cur.execute(
        "SELECT COUNT(*) AS cnt FROM bookings WHERE user_id = %s AND status = 'отменена'",
        (user_id,),
    )
    cancelled = cur.fetchone()["cnt"]

    cur.execute(
        """SELECT COUNT(DISTINCT b.id) AS cnt
           FROM bookings b
           JOIN payments p ON p.booking_id = b.id
           WHERE b.user_id = %s AND p.status = 'подтверждена'""",
        (user_id,),
    )
    paid = cur.fetchone()["cnt"]

    cur.execute(
        "SELECT COUNT(*) AS cnt FROM bookings WHERE user_id = %s AND status = 'посещена'",
        (user_id,),
    )
    attended = cur.fetchone()["cnt"]

    cur.close()
    conn.close()

    active_total = total - cancelled
    attendance_rate = round(attended / active_total * 100) if active_total > 0 else 0
    hours_played = round(attended * 1.5, 1)

    return {
        "total": total,
        "paid": paid,
        "attended": attended,
        "cancelled": cancelled,
        "attendance_rate": attendance_rate,
        "hours_played": hours_played,
    }


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


def get_payments_filtered(search: str = "", status: str = ""):
    """Оплаты с фильтрацией по поиску и статусу."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT p.*, u.name AS user_name, g.game_date, g.game_time, g.location
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE 1=1
    """
    params = []

    if search:
        query += " AND u.name ILIKE %s"
        params.append(f"%{search}%")

    if status:
        query += " AND p.status = %s"
        params.append(status)

    query += " ORDER BY p.created_at DESC"

    cur.execute(query, params)
    payments = cur.fetchall()
    cur.close()
    conn.close()
    return payments


def get_payments_paginated(search: str = "", status: str = "", page: int = 1, per_page: int = 20):
    """Оплаты с фильтрацией и пагинацией — для CRM."""
    page = max(1, page)
    offset = (page - 1) * per_page

    where_clause = "WHERE 1=1"
    params = []

    if search:
        where_clause += " AND u.name ILIKE %s"
        params.append(f"%{search}%")

    if status:
        where_clause += " AND p.status = %s"
        params.append(status)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        {where_clause}
        """,
        params,
    )
    total = cur.fetchone()["cnt"]

    cur.execute(
        f"""
        SELECT p.*, u.name AS user_name, g.game_date, g.game_time, g.location
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        {where_clause}
        ORDER BY p.created_at DESC
        LIMIT %s OFFSET %s
        """,
        params + [per_page, offset],
    )
    payments = cur.fetchall()
    cur.close()
    conn.close()

    return _paginated_result(payments, total, page, per_page)


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


# ---------------------------------------------------------------------------
# VISITS — посещения
# ---------------------------------------------------------------------------

def mark_booking_visited(booking_id: int):
    """Отмечает заявку как посещённую."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE bookings SET status = 'посещена' WHERE id = %s", (booking_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_all_visits():
    """Все посещения с данными об игроках и играх."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE b.status = 'посещена'
        ORDER BY g.game_date DESC, g.game_time DESC
    """)
    visits = cur.fetchall()
    cur.close()
    conn.close()
    return visits


def get_visits_paginated(page: int = 1, per_page: int = 20):
    """Посещения с пагинацией — для CRM."""
    page = max(1, page)
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM bookings WHERE status = 'посещена'")
    total = cur.fetchone()["cnt"]

    cur.execute(
        """
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE b.status = 'посещена'
        ORDER BY g.game_date DESC, g.game_time DESC
        LIMIT %s OFFSET %s
        """,
        (per_page, offset),
    )
    visits = cur.fetchall()
    cur.close()
    conn.close()

    return _paginated_result(visits, total, page, per_page)


# ---------------------------------------------------------------------------
# CLUBS — клубы/площадки
# ---------------------------------------------------------------------------

def create_club(name: str, address: str, phone: str, description: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO clubs (name, address, phone, description)
           VALUES (%s, %s, %s, %s) RETURNING *""",
        (name, address, phone, description),
    )
    club = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return club


def get_all_clubs():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM clubs ORDER BY name")
    clubs = cur.fetchall()
    cur.close()
    conn.close()
    return clubs


def get_clubs_paginated(page: int = 1, per_page: int = 20):
    """Клубы с пагинацией — для CRM."""
    page = max(1, page)
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM clubs")
    total = cur.fetchone()["cnt"]

    cur.execute("SELECT * FROM clubs ORDER BY name LIMIT %s OFFSET %s", (per_page, offset))
    clubs = cur.fetchall()
    cur.close()
    conn.close()

    return _paginated_result(clubs, total, page, per_page)


def get_club_by_id(club_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM clubs WHERE id = %s", (club_id,))
    club = cur.fetchone()
    cur.close()
    conn.close()
    return club


def update_club(club_id: int, name: str, address: str, phone: str, description: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE clubs
           SET name = %s, address = %s, phone = %s, description = %s
           WHERE id = %s""",
        (name, address, phone, description, club_id),
    )
    conn.commit()
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# ACTION LOGS — журнал действий
# ---------------------------------------------------------------------------

def log_action(action: str, entity_type: str = None, entity_id: int = None, details: str = None):
    """Записывает действие в журнал."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO action_logs (action, entity_type, entity_id, details)
           VALUES (%s, %s, %s, %s)""",
        (action, entity_type, entity_id, details),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_all_logs():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM action_logs ORDER BY created_at DESC LIMIT 100")
    logs = cur.fetchall()
    cur.close()
    conn.close()
    return logs


def get_logs_paginated(page: int = 1, per_page: int = 20):
    """Журнал действий с пагинацией — для CRM."""
    page = max(1, page)
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM action_logs")
    total = cur.fetchone()["cnt"]

    cur.execute(
        "SELECT * FROM action_logs ORDER BY created_at DESC LIMIT %s OFFSET %s",
        (per_page, offset),
    )
    logs = cur.fetchall()
    cur.close()
    conn.close()

    return _paginated_result(logs, total, page, per_page)


def count_pending_payments() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM payments WHERE status = 'ожидает'")
    total = cur.fetchone()["cnt"]
    cur.close()
    conn.close()
    return total


def count_active_bookings() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM bookings WHERE status != 'отменена'")
    total = cur.fetchone()["cnt"]
    cur.close()
    conn.close()
    return total


def count_visits() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM bookings WHERE status = 'посещена'")
    total = cur.fetchone()["cnt"]
    cur.close()
    conn.close()
    return total


def count_clubs() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM clubs")
    total = cur.fetchone()["cnt"]
    cur.close()
    conn.close()
    return total


def count_logs() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS cnt FROM action_logs")
    total = cur.fetchone()["cnt"]
    cur.close()
    conn.close()
    return total


# ---------------------------------------------------------------------------
# CLUB INFO — информация о клубе
# ---------------------------------------------------------------------------

def get_club_info():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM club_info ORDER BY id DESC LIMIT 1")
    info = cur.fetchone()
    cur.close()
    conn.close()
    return info


def update_club_info(name: str, description: str, contact_phone: str, contact_email: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE club_info
           SET name = %s, description = %s, contact_phone = %s, contact_email = %s, updated_at = NOW()
           WHERE id = (SELECT id FROM club_info ORDER BY id DESC LIMIT 1)""",
        (name, description, contact_phone, contact_email),
    )
    conn.commit()
    cur.close()
    conn.close()
