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

import logging
import os
import threading

import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Пул соединений. Раньше get_connection() делал psycopg2.connect(...) на
# КАЖДЫЙ вызов, а почти каждая функция в этом файле открывает и закрывает
# своё собственное соединение — то есть один клик в CRM, дёргающий несколько
# функций (например смена статуса заявки: get_booking_by_id + update_booking_status
# + get_payment_for_booking + create_payment), устанавливал 3-6 НОВЫХ TCP+TLS
# соединений с БД. На управляемом Postgres (Neon и т.п.) один такой хендшейк
# занимает ~2-3 секунды — отсюда и «кнопка отвечает 10 секунд». Запрос на уже
# открытом соединении занимает в десятки раз меньше.
# minconn=2 держит пару соединений всегда открытыми (прогрето при первом
# обращении), maxconn=10 — как и в асинхронном пуле бота (database_async.py).
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))

_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is None:
            if not DATABASE_URL:
                raise RuntimeError("Не найден DATABASE_URL. Проверьте файл .env")
            _pool = psycopg2_pool.ThreadedConnectionPool(
                DB_POOL_MIN_SIZE, DB_POOL_MAX_SIZE, DATABASE_URL,
                cursor_factory=RealDictCursor,
            )
    return _pool


class _PooledConnection:
    """Обёртка над соединением из пула с тем же интерфейсом, что и обычный
    psycopg2-connection (.cursor()/.commit()/.rollback()/...), — благодаря
    этому все ~60 функций ниже, написанные в стиле
    `conn = get_connection(); ...; conn.close()`, продолжают работать без
    изменений. Единственная разница — .close() не рвёт TCP-соединение, а
    возвращает его в пул, чтобы следующий вызов get_connection() получил уже
    прогретое соединение.

    detached=True — соединение НЕ из пула (см. get_connection(): fallback
    при истощении пула), .close() просто закрывает его как обычное
    psycopg2-соединение, ничего не возвращая."""

    def __init__(self, pool_, conn, detached=False):
        self._pool = pool_
        self._conn = conn
        self._returned = False
        self._detached = detached

    def close(self):
        if self._returned:
            return
        self._returned = True
        if self._detached:
            try:
                self._conn.close()
            except Exception:
                pass
            return
        # Если соединение "протухло" (например, БД перезапускалась/сеть
        # моргнула) — не кладём его обратно в пул, иначе следующий getconn()
        # унаследует уже мёртвое соединение и упадёт на первом же запросе.
        broken = bool(self._conn.closed)
        try:
            self._pool.putconn(self._conn, close=broken)
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_connection():
    """Берёт соединение из пула (пул создаётся при первом обращении).
    RealDictCursor — чтобы результаты запросов приходили в виде словарей
    (например row["name"] вместо row[0]), это удобнее для новичка.

    Fallback при истощении пула: если какая-то функция забыла закрыть
    соединение (утечка после необработанного исключения) и все maxconn
    заняты, pool.getconn() бросает PoolError — раньше это означало
    мгновенный 500 на КАЖДЫЙ следующий запрос к CRM, то есть полный отказ
    сайта до перезапуска процесса. Теперь в этом случае открываем отдельное
    "внепуловое" соединение напрямую — запрос станет медленнее (новый
    TCP+TLS хендшейк, как до внедрения пула), но не откажет полностью."""
    pool_ = _get_pool()
    try:
        conn = pool_.getconn()
    except psycopg2_pool.PoolError:
        logger.error(
            "Пул соединений с БД истощён (maxconn=%s) — открываю соединение "
            "напрямую, в обход пула. Похоже на утечку соединений.",
            DB_POOL_MAX_SIZE,
        )
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return _PooledConnection(pool_, conn, detached=True)
    return _PooledConnection(pool_, conn)


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


def _paginated_from_window(cur, rows, page, per_page, fallback_query, fallback_params=()):
    """Достаёт total из total_count (COUNT(*) OVER(), посчитан в том же
    запросе, что и сами строки страницы — 1 round-trip к БД вместо 2).

    Если страница пустая (например, запросили page за пределами
    существующих данных) — окно ничего не считает, потому что строк нет.
    В этом (редком) случае делаем отдельный COUNT(*), чтобы пагинация
    осталась корректной."""
    if rows:
        total = rows[0]["total_count"]
    else:
        cur.execute(fallback_query, fallback_params)
        total = cur.fetchone()["cnt"]
    return _paginated_result(rows, total, page, per_page)


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
            city TEXT,
            club_id INTEGER,
            address TEXT,
            duration_minutes INTEGER NOT NULL DEFAULT 90,
            level TEXT,
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

    # Таблицу admin_logs (и переименование старой action_logs в неё) создаёт
    # _migrate_admin_logs_table() внутри migrate_db() — см. её вызов сразу
    # после init_db() в init_db.py. Если создать admin_logs прямо здесь, при
    # обновлении существующей базы (где ещё есть action_logs) получилась бы
    # пустая admin_logs ДО переименования, и данные старого журнала "потерялись"
    # бы за новой (изначально пустой) таблицей.

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


def _migrate_games_table(cur):
    """reminder_2h_sent — отдельный флаг для напоминания за 2 часа до игры,
    независимый от reminder_sent (используется для напоминания за 24 часа —
    имя оставлено как есть, чтобы не переименовывать существующий столбец и
    не ломать старые данные/индекс на нём).

    city/club_id/address/duration_minutes/level — расширенные поля формы
    игры в CRM (город, привязка к клубу из таблицы clubs, точный адрес,
    длительность и уровень). location продолжает храниться и заполняться
    (из city+клуб+address) — его читают бот и старые запросы/шаблоны,
    поэтому мы не убираем колонку, а просто перестаём считать её единственным
    источником данных о месте проведения."""
    cur.execute(
        "ALTER TABLE games ADD COLUMN IF NOT EXISTS reminder_2h_sent BOOLEAN NOT NULL DEFAULT FALSE;"
    )
    cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS city TEXT;")
    cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS club_id INTEGER;")
    cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS address TEXT;")
    cur.execute(
        "ALTER TABLE games ADD COLUMN IF NOT EXISTS duration_minutes INTEGER NOT NULL DEFAULT 90;"
    )
    cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS level TEXT;")


def _migrate_admin_logs_table(cur):
    """action_logs -> admin_logs: более точное имя (журнал действий именно
    администратора CRM), плюс новые колонки old_value/new_value — состояние
    сущности до и после действия (в дополнение к произвольному текстовому
    details, который использовался раньше)."""
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'action_logs')
               AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'admin_logs') THEN
                ALTER TABLE action_logs RENAME TO admin_logs;
            END IF;
        END $$;
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_logs (
            id SERIAL PRIMARY KEY,
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id INTEGER,
            old_value TEXT,
            new_value TEXT,
            details TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    cur.execute("ALTER TABLE admin_logs ADD COLUMN IF NOT EXISTS old_value TEXT;")
    cur.execute("ALTER TABLE admin_logs ADD COLUMN IF NOT EXISTS new_value TEXT;")


def _migrate_bookings_table(cur):
    """slots_count — сколько мест занимает одна заявка (фича «Сколько мест?
    (1-4)» в боте). DEFAULT 1 — старые заявки, созданные до этой миграции,
    корректно продолжают считаться как 1 занятое место."""
    cur.execute(
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS slots_count INTEGER NOT NULL DEFAULT 1;"
    )


def _migrate_payments_table(cur):
    """method — способ оплаты, выбранный игроком в боте (card/sbp), для
    отображения в CRM. NULL — способ ещё не выбран (например, заявку
    оплатили наличными на месте без похода через бота)."""
    cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS method TEXT;")


def _create_indexes(cur):
    """Создаёт индексы для самых частых и тяжёлых запросов.
    CREATE INDEX IF NOT EXISTS — безопасно вызывать повторно."""
    index_statements = [
        # games: список ближайших игр сортируется/фильтруется по дате и времени
        "CREATE INDEX IF NOT EXISTS idx_games_date_time ON games (game_date, game_time);",
        "CREATE INDEX IF NOT EXISTS idx_games_reminder_pending ON games (reminder_sent) WHERE reminder_sent = FALSE;",
        "CREATE INDEX IF NOT EXISTS idx_games_reminder_2h_pending ON games (reminder_2h_sent) WHERE reminder_2h_sent = FALSE;",
        # список игр в CRM фильтруется по уровню (см. games_list в app.py)
        "CREATE INDEX IF NOT EXISTS idx_games_level ON games (level);",
        "CREATE INDEX IF NOT EXISTS idx_games_club_id ON games (club_id);",

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

        # admin_logs: журнал всегда читается с ORDER BY created_at DESC
        "CREATE INDEX IF NOT EXISTS idx_admin_logs_created_at ON admin_logs (created_at DESC);",

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
    _migrate_games_table(cur)
    _migrate_bookings_table(cur)
    _migrate_payments_table(cur)
    _migrate_admin_logs_table(cur)
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


def get_games_paginated(page: int = 1, per_page: int = 20, level: str = ""):
    """Список игр с пагинацией + количество занятых мест/собранных оплат
    одним запросом (без N+1), с опциональным фильтром по уровню.
    Возвращает dict с items/total/total_pages."""
    page = max(1, page)
    offset = (page - 1) * per_page

    where_clause = "WHERE 1=1"
    params: list = []
    if level:
        where_clause += " AND g.level = %s"
        params.append(level)

    conn = get_connection()
    cur = conn.cursor()

    # total_count = COUNT(*) OVER() считается в том же запросе, что и сами
    # строки страницы — 1 round-trip к БД вместо 2 (COUNT(*) отдельно +
    # SELECT страницы). См. _paginated_from_window.
    cur.execute(
        f"""
        SELECT g.*,
               c.name AS club_name,
               COALESCE(bk.taken, 0) AS taken,
               COALESCE(pm.collected, 0) AS collected,
               COUNT(*) OVER() AS total_count
        FROM games g
        LEFT JOIN clubs c ON c.id = g.club_id
        LEFT JOIN (
            -- SUM(slots_count), а не COUNT(*): одна заявка теперь может
            -- занимать сразу несколько мест (фича «Сколько мест? (1-4)»
            -- в боте), поэтому число заявок больше не равно числу мест.
            SELECT game_id, SUM(slots_count) AS taken
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
        {where_clause}
        ORDER BY g.game_date DESC, g.game_time DESC
        LIMIT %s OFFSET %s
        """,
        params + [per_page, offset],
    )
    games = cur.fetchall()
    result = _paginated_from_window(
        cur, games, page, per_page,
        f"SELECT COUNT(*) AS cnt FROM games g {where_clause}", params,
    )
    cur.close()
    conn.close()

    return result


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
            SELECT game_id, SUM(slots_count) AS taken
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


def create_game(
    game_date, game_time, location, price, total_slots,
    city=None, club_id=None, address=None, duration_minutes=90, level=None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO games (
               game_date, game_time, location, price, total_slots,
               city, club_id, address, duration_minutes, level
           )
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (game_date, game_time, location, price, total_slots,
         city, club_id, address, duration_minutes, level),
    )
    game = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return game


def update_game(
    game_id, game_date, game_time, location, price, total_slots,
    city=None, club_id=None, address=None, duration_minutes=90, level=None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE games
           SET game_date = %s, game_time = %s, location = %s,
               price = %s, total_slots = %s,
               city = %s, club_id = %s, address = %s,
               duration_minutes = %s, level = %s
           WHERE id = %s
           RETURNING *""",
        (game_date, game_time, location, price, total_slots,
         city, club_id, address, duration_minutes, level, game_id),
    )
    game = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return game


def delete_game(game_id: int):
    """Удаляет игру. bookings/payments удалятся автоматически (ON DELETE
    CASCADE в схеме), поэтому явно чистить их отдельно не нужно."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM games WHERE id = %s", (game_id,))
    conn.commit()
    cur.close()
    conn.close()


def count_bookings_for_game(game_id: int) -> int:
    """Сколько мест занято активными (не отменёнными) заявками на игру."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(SUM(slots_count), 0) AS cnt FROM bookings WHERE game_id = %s AND status != 'отменена'",
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


def update_booking_status_and_get(booking_id: int, status: str):
    """Меняет статус заявки и возвращает обновлённую строку + старый статус
    за 1 round-trip (вместо отдельных get_booking_by_id + update_booking_status).
    old_status берётся из подзапроса на состояние ДО обновления — Postgres
    вычисляет его атомарно в рамках одного UPDATE, так что это безопасно."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bookings AS b
        SET status = %s
        FROM (SELECT status FROM bookings WHERE id = %s) AS old
        WHERE b.id = %s
        RETURNING b.*, old.status AS old_status
        """,
        (status, booking_id, booking_id),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


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
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location,
               COUNT(*) OVER() AS total_count
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
    result = _paginated_from_window(
        cur, bookings, page, per_page,
        f"""
        SELECT COUNT(*) AS cnt
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        {where_clause}
        """,
        params,
    )
    cur.close()
    conn.close()

    return result


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


def get_payment_for_booking(booking_id: int):
    """Есть ли уже оплата для этой заявки. Начиная с фичи оплаты через бота
    (см. bot.py) запись о платеже создаётся сразу при бронировании — эта
    функция нужна, чтобы CRM не создавала дублирующую оплату, когда
    администратор вручную подтверждает заявку (см. booking_update_status
    в app.py)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM payments WHERE booking_id = %s ORDER BY id LIMIT 1",
        (booking_id,),
    )
    payment = cur.fetchone()
    cur.close()
    conn.close()
    return payment


def get_payment_check_for_booking(booking_id: int):
    """Всё, что нужно booking_update_status() для решения "создавать ли
    оплату при подтверждении заявки" — есть ли уже payment, цена игры и
    slots_count — за 1 round-trip вместо трёх отдельных (get_payment_for_booking
    + get_booking_by_id + get_game_by_id)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT b.slots_count, g.price AS game_price, p.id AS payment_id
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        LEFT JOIN payments p ON p.booking_id = b.id
        WHERE b.id = %s
        ORDER BY p.id
        LIMIT 1
        """,
        (booking_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


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
        SELECT p.*, u.name AS user_name, g.game_date, g.game_time, g.location,
               COUNT(*) OVER() AS total_count
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
    result = _paginated_from_window(
        cur, payments, page, per_page,
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
    cur.close()
    conn.close()

    return result


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

    cur.execute(
        """
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.game_date, g.game_time, g.location,
               COUNT(*) OVER() AS total_count
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
    result = _paginated_from_window(
        cur, visits, page, per_page,
        "SELECT COUNT(*) AS cnt FROM bookings WHERE status = 'посещена'",
    )
    cur.close()
    conn.close()

    return result


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

    cur.execute(
        "SELECT *, COUNT(*) OVER() AS total_count FROM clubs ORDER BY name LIMIT %s OFFSET %s",
        (per_page, offset),
    )
    clubs = cur.fetchall()
    result = _paginated_from_window(
        cur, clubs, page, per_page, "SELECT COUNT(*) AS cnt FROM clubs",
    )
    cur.close()
    conn.close()

    return result


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
# ADMIN LOGS — журнал действий администратора
# ---------------------------------------------------------------------------

def log_action(
    action: str,
    entity_type: str = None,
    entity_id: int = None,
    old_value: str = None,
    new_value: str = None,
    details: str = None,
):
    """Записывает действие администратора в журнал (admin_logs).

    old_value/new_value — строковое (обычно JSON) представление сущности до
    и после изменения; details — свободный текст, если нужен доп. комментарий."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO admin_logs (action, entity_type, entity_id, old_value, new_value, details)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (action, entity_type, entity_id, old_value, new_value, details),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_all_logs():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT 100")
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

    cur.execute(
        "SELECT *, COUNT(*) OVER() AS total_count FROM admin_logs ORDER BY created_at DESC LIMIT %s OFFSET %s",
        (per_page, offset),
    )
    logs = cur.fetchall()
    result = _paginated_from_window(
        cur, logs, page, per_page, "SELECT COUNT(*) AS cnt FROM admin_logs",
    )
    cur.close()
    conn.close()

    return result


def get_dashboard_summary() -> dict:
    """Все 6 счётчиков главной страницы CRM одним запросом (1 round-trip к
    БД вместо 6 — раньше index() дёргал count_games/count_active_bookings/
    count_pending_payments/count_visits/count_clubs/count_logs отдельно,
    и это было самой медленной страницей CRM)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM games) AS games,
            (SELECT COUNT(*) FROM bookings WHERE status != 'отменена') AS bookings,
            (SELECT COUNT(*) FROM payments WHERE status = 'ожидает') AS pending_payments,
            (SELECT COUNT(*) FROM bookings WHERE status = 'посещена') AS visits,
            (SELECT COUNT(*) FROM clubs) AS clubs,
            (SELECT COUNT(*) FROM admin_logs) AS logs
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)


def get_latest_activity_marker() -> dict:
    """Лёгкий «маркер свежести» для заявок и оплат: max(id) + max(updated_at)
    по bookings/payments. Используется CRM-страницами для поллинга —
    сравниваем с тем, что было при рендере, и если что-то изменилось (новая
    заявка/оплата из бота или правка другим админом), подсказываем
    обновить страницу. Один простой запрос, не нагружает БД при частом
    опросе (индексы по id есть по умолчанию — это PK)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (SELECT COALESCE(MAX(id), 0) FROM bookings) AS max_booking_id,
            (SELECT COALESCE(MAX(id), 0) FROM payments) AS max_payment_id,
            (SELECT COUNT(*) FROM bookings) AS bookings_count,
            (SELECT COUNT(*) FROM payments) AS payments_count
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)


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
    cur.execute("SELECT COUNT(*) AS cnt FROM admin_logs")
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
