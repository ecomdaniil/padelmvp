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
import time

import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# game_date/game_time — наивные «настенные» часы клуба (Москва). Сессия
# Postgres обычно в UTC, поэтому сырой NOW() нельзя сравнивать с ними —
# будет сдвиг ~3 часа. Для «сейчас» относительно игр всегда Москва:
APP_TIMEZONE = "Europe/Moscow"
_LOCAL_NOW_EXPR = f"(NOW() AT TIME ZONE '{APP_TIMEZONE}')"
_LOCAL_TODAY_EXPR = f"(({_LOCAL_NOW_EXPR})::date)"

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
                # Не висим минутами, если Neon/сеть недоступны — иначе
                # gunicorn worker не отвечает на /health и Render «белый экран».
                connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
                # TCP keepalive — чтобы Neon/прокси не роняли «тихие»
                # соединения из пула между запросами CRM.
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
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


_keepalive_started = False
_keepalive_lock = threading.Lock()


def _ping_db() -> None:
    """Один лёгкий SELECT 1 — прогрев пула / анти-suspend Neon."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
    finally:
        conn.close()


def start_keepalive_thread(interval_seconds: int = 45) -> None:
    """Фоновый поток, держащий БД "тёплой" для CRM — тот же смысл, что и
    database_async.keepalive_loop() для бота: Neon приостанавливает
    вычислительный узел после простоя, и без периодических запросов первая
    открытая после паузы страница CRM ждала бы "холодный старт" (секунды).

    Сразу при старте делает ping (не ждёт interval), затем каждые
    interval_seconds (по умолчанию 45с — с запасом меньше типичного
    suspend Neon ~5 мин). Idempotent."""
    global _keepalive_started
    with _keepalive_lock:
        if _keepalive_started:
            return
        _keepalive_started = True

    def _loop():
        while True:
            try:
                _ping_db()
            except Exception:
                # Сеть/БД могли на секунду моргнуть — следующая попытка
                # через interval_seconds всё исправит, поток не должен упасть.
                pass
            time.sleep(interval_seconds)

    threading.Thread(target=_loop, daemon=True, name="db-keepalive").start()


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
            city TEXT NOT NULL DEFAULT '',
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
            city TEXT NOT NULL DEFAULT '',
            address TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    # Создаём запись о клубе, если её нет
    cur.execute("""
        INSERT INTO club_info (name, description, contact_phone, contact_email, city, address)
        SELECT 'Padel Club', 'Добро пожаловать в наш клуб падел!', '', '', '', ''
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
        # @username из Telegram — для состава игры в боте и уведомлений.
        ("telegram_username", "TEXT"),
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
    # Ручная корректировка занятых мест админом в CRM (например, офлайн-запись
    # игрока без брони в системе) — независима от реального числа броней,
    # которое всегда считается из bookings (см. get_games_paginated/taken).
    cur.execute(
        "ALTER TABLE games ADD COLUMN IF NOT EXISTS booked_places INTEGER NOT NULL DEFAULT 0;"
    )
    # Предупреждение за 3ч и автоотмена за 1ч при недоборе состава.
    cur.execute(
        "ALTER TABLE games ADD COLUMN IF NOT EXISTS underfill_warn_3h_sent "
        "BOOLEAN NOT NULL DEFAULT FALSE;"
    )
    cur.execute(
        "ALTER TABLE games ADD COLUMN IF NOT EXISTS underfill_cancelled "
        "BOOLEAN NOT NULL DEFAULT FALSE;"
    )
    # event_type: 'game' (обычная игра) | 'training' (тренировка с тренером).
    # title — название тренировки («Отработка ударов»); coach_id — тренер.
    cur.execute(
        "ALTER TABLE games ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'game';"
    )
    cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS title TEXT;")
    cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS coach_id INTEGER;")


def _migrate_coaches_table(cur):
    """Тренеры клуба — карточки для CRM и раздела «Тренеры» в боте."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS coaches (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT NOT NULL DEFAULT '',
            telegram_username TEXT NOT NULL DEFAULT '',
            experience TEXT NOT NULL DEFAULT '',
            specialization TEXT NOT NULL DEFAULT '',
            achievements TEXT NOT NULL DEFAULT '',
            emoji TEXT NOT NULL DEFAULT '🧑‍🏫',
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """
    )
    # Сиды — те же три тренера, что раньше были захардкожены в bot_content.
    cur.execute("SELECT COUNT(*) AS cnt FROM coaches")
    if int(cur.fetchone()["cnt"] or 0) == 0:
        seeds = [
            (
                "Алексей Иванов",
                "",
                "",
                "8 лет",
                "Сертифицированный тренер FIP. Специализируется на обучении новичков "
                "и отработке базовой техники ударов.",
                "• Тренерский стаж — 8 лет\n"
                "• Победитель регионального турнира 2023\n"
                "• 200+ учеников прошли обучение",
                "🧑‍🏫",
                1,
            ),
            (
                "Мария Петрова",
                "",
                "",
                "5 лет",
                "Мастер спорта по теннису, перешла в падел 5 лет назад. "
                "Ведёт групповые и индивидуальные тренировки для любителей и продвинутых.",
                "• Участница Кубка России по падел 2024\n"
                "• Автор курса «Падел с нуля за 4 недели»\n"
                "• Рейтинг FIP — 4.5",
                "👩‍🏫",
                2,
            ),
            (
                "Дмитрий Соколов",
                "",
                "",
                "Стажировка в Испании",
                "Тренер по тактике и игре у сетки. Помогает парам выстроить "
                "командную стратегию и улучшить результаты на турнирах.",
                "• 3-кратный чемпион городской лиги\n"
                "• Тренер команды-финалиста Кубка клубов\n"
                "• Стажировка в академии падел, Испания",
                "🧑‍🏫",
                3,
            ),
        ]
        for row in seeds:
            cur.execute(
                """
                INSERT INTO coaches (
                    name, phone, telegram_username, experience,
                    specialization, achievements, emoji, sort_order
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row,
            )


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
    # description — человекочитаемый текст на русском ("Игра №9 создана: ...")
    # для отображения в /logs. Раньше туда же пытались показывать old_value/
    # new_value (JSON всей строки сущности) — нечитаемо для админа. Старые
    # записи (созданные до этого изменения) description не имеют — logs.html
    # для них показывает old_value/new_value как раньше, с пометкой
    # «устаревший формат».
    cur.execute("ALTER TABLE admin_logs ADD COLUMN IF NOT EXISTS description TEXT;")


def _migrate_bookings_table(cur):
    """slots_count — сколько мест занимает одна заявка (фича «Сколько мест?
    (1-4)» в боте). DEFAULT 1 — старые заявки, созданные до этой миграции,
    корректно продолжают считаться как 1 занятое место.

    admin_notify_message_id — message_id уведомления «Новая запись на корт»
    в чате админа (Telegram). Нужен, чтобы при отмене ДО оплаты бот мог
    удалить это сообщение (см. _notify_admin_new_booking / cancel в bot.py).

    admin_extra_notify_message_id — то же для «Докупка мест»: при отмене
    только доплаты удаляем это сообщение, исходную запись оставляем."""
    cur.execute(
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS slots_count INTEGER NOT NULL DEFAULT 1;"
    )
    cur.execute(
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS admin_notify_message_id BIGINT;"
    )
    cur.execute(
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS admin_extra_notify_message_id BIGINT;"
    )
    # no_show — админ отметил «Не был» в CRM «Посещения»; в статистике бота
    # такое посещение не засчитывается.
    cur.execute(
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS no_show BOOLEAN NOT NULL DEFAULT FALSE;"
    )


def _migrate_payments_table(cur):
    """method — способ оплаты, выбранный игроком в боте (card/sbp), для
    отображения в CRM. NULL — способ ещё не выбран (например, заявку
    оплатили наличными на месте без похода через бота).

    player_notified_at — момент, когда игрок нажал «✅ Я оплатил» в боте
    (см. process_paid_notify в bot.py). NULL — платёж создан (сразу при
    записи на игру, статус «ожидает»), но игрок ещё не сообщал об оплате.
    Бейдж «+N» рядом с «Оплаты» в шапке CRM (см. count_new_since) считает
    ТОЛЬКО платежи с заполненным player_notified_at — иначе бейдж загорался
    бы на каждую новую заявку, даже если игрок ещё даже не открывал экран
    оплаты."""
    cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS method TEXT;")
    cur.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS player_notified_at TIMESTAMP;")
    # admin_attention_at — момент, когда оплата снова требует внимания админа
    # в CRM (игрок сообщил об оплате ИЛИ статус стал «возврат»). Бейдж «+N»
    # у «Оплаты» смотрит на это поле, а не только на player_notified_at —
    # иначе отмена оплаченной записи не подсвечивала возврат.
    cur.execute(
        "ALTER TABLE payments ADD COLUMN IF NOT EXISTS admin_attention_at TIMESTAMP;"
    )
    cur.execute(
        """
        UPDATE payments
           SET admin_attention_at = player_notified_at
         WHERE admin_attention_at IS NULL
           AND player_notified_at IS NOT NULL
        """
    )
    cur.execute(
        """
        UPDATE payments
           SET admin_attention_at = COALESCE(admin_attention_at, NOW())
         WHERE status = 'возврат'
           AND admin_attention_at IS NULL
        """
    )
    # ЮKassa: id платежа у провайдера и ссылка на оплату (confirmation_url).
    cur.execute(
        "ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_payment_id TEXT;"
    )
    cur.execute(
        "ALTER TABLE payments ADD COLUMN IF NOT EXISTS confirmation_url TEXT;"
    )
    # Разово подчищаем «висящие» оплаты по уже отменённым заявкам — они
    # больше не должны отображаться в разделе «Оплаты» CRM.
    cur.execute(
        """
        DELETE FROM payments p
        USING bookings b
        WHERE p.booking_id = b.id
          AND b.status = 'отменена'
          AND p.status = 'ожидает'
        """
    )


def _create_indexes(cur):
    """Создаёт индексы для самых частых и тяжёлых запросов.
    CREATE INDEX IF NOT EXISTS — безопасно вызывать повторно."""
    index_statements = [
        # games: список ближайших игр сортируется/фильтруется по дате и времени
        "CREATE INDEX IF NOT EXISTS idx_games_date_time ON games (game_date, game_time);",
        "CREATE INDEX IF NOT EXISTS idx_games_reminder_pending ON games (reminder_sent) WHERE reminder_sent = FALSE;",
        "CREATE INDEX IF NOT EXISTS idx_games_reminder_2h_pending ON games (reminder_2h_sent) WHERE reminder_2h_sent = FALSE;",
        "CREATE INDEX IF NOT EXISTS idx_games_underfill_warn ON games (underfill_warn_3h_sent) WHERE underfill_warn_3h_sent = FALSE;",
        "CREATE INDEX IF NOT EXISTS idx_games_underfill_active ON games (game_date, game_time) WHERE underfill_cancelled = FALSE;",
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
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_provider_id "
        "ON payments (provider_payment_id) WHERE provider_payment_id IS NOT NULL;",
        # Автоотмена неоплаченных счетов (см. expire_unpaid_payment в database_async)
        "CREATE INDEX IF NOT EXISTS idx_payments_unpaid_timeout "
        "ON payments (created_at) "
        "WHERE status = 'ожидает' AND player_notified_at IS NULL;",

        # admin_logs: журнал всегда читается с ORDER BY created_at DESC
        "CREATE INDEX IF NOT EXISTS idx_admin_logs_created_at ON admin_logs (created_at DESC);",

        # reviews: список отзывов сортируется по дате
        "CREATE INDEX IF NOT EXISTS idx_reviews_created_at ON reviews (created_at DESC);",
    ]
    for statement in index_statements:
        cur.execute(statement)

    # Один «открытый» неоплаченный счёт на бронь (ещё без player_notified_at).
    # После оплаты через PayMaster счёт остаётся «ожидает» до подтверждения
    # админом — на такую бронь должна уметь создаваться отдельная доплата
    # при «Докупить места», поэтому уникальность только на open-pending.
    cur.execute("DROP INDEX IF EXISTS idx_payments_one_pending;")
    cur.execute(
        """
        DELETE FROM payments a
        USING payments b
        WHERE a.booking_id = b.booking_id
          AND a.status = 'ожидает'
          AND a.player_notified_at IS NULL
          AND b.status = 'ожидает'
          AND b.player_notified_at IS NULL
          AND a.id < b.id
        """
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_one_open_pending "
        "ON payments (booking_id) "
        "WHERE status = 'ожидает' AND player_notified_at IS NULL"
    )


def _migrate_clubs_table(cur):
    """city — город площадки; при выборе клуба в форме игры подставляется
    вместе с адресом."""
    cur.execute(
        "ALTER TABLE clubs ADD COLUMN IF NOT EXISTS city TEXT NOT NULL DEFAULT '';"
    )


def _migrate_club_info_table(cur):
    """admin_telegram_id / username — уведомления бота; city/address — площадка
    для автоподстановки в формах игр и тренировок (через sync в clubs)."""
    cur.execute(
        "ALTER TABLE club_info ADD COLUMN IF NOT EXISTS admin_telegram_id BIGINT;"
    )
    cur.execute(
        "ALTER TABLE club_info ADD COLUMN IF NOT EXISTS admin_telegram_username TEXT;"
    )
    cur.execute(
        "ALTER TABLE club_info ADD COLUMN IF NOT EXISTS city TEXT NOT NULL DEFAULT '';"
    )
    cur.execute(
        "ALTER TABLE club_info ADD COLUMN IF NOT EXISTS address TEXT NOT NULL DEFAULT '';"
    )
    # Какие поля «О клубе» показывать в боте (галочки в CRM).
    for col, default in (
        ("bot_show_name", "TRUE"),
        ("bot_show_city", "TRUE"),
        ("bot_show_address", "TRUE"),
        ("bot_show_description", "TRUE"),
        ("bot_show_phone", "TRUE"),
        ("bot_show_email", "FALSE"),
        ("bot_show_admin_username", "FALSE"),
    ):
        cur.execute(
            f"ALTER TABLE club_info ADD COLUMN IF NOT EXISTS {col} BOOLEAN NOT NULL DEFAULT {default};"
        )
    # Если город/адрес ещё пустые — подтянуть из первой площадки clubs.
    cur.execute(
        """
        UPDATE club_info AS ci
        SET city = COALESCE(NULLIF(ci.city, ''), c.city, ''),
            address = COALESCE(NULLIF(ci.address, ''), c.address, '')
        FROM (
            SELECT city, address FROM clubs ORDER BY id ASC LIMIT 1
        ) AS c
        WHERE ci.id = (SELECT id FROM club_info ORDER BY id DESC LIMIT 1)
          AND (COALESCE(ci.city, '') = '' OR COALESCE(ci.address, '') = '')
        """
    )


def migrate_db():
    """Запускает миграции без пересоздания таблиц. Безопасно вызывать повторно."""
    conn = get_connection()
    cur = conn.cursor()
    _migrate_users_table(cur)
    _migrate_coaches_table(cur)
    _migrate_games_table(cur)
    _migrate_bookings_table(cur)
    _migrate_payments_table(cur)
    _migrate_admin_logs_table(cur)
    _migrate_clubs_table(cur)
    _migrate_club_info_table(cur)
    # Подтянуть игры/тренировки к актуальной площадке из «О клубе».
    cur.execute(
        """SELECT name, city, address, contact_phone
           FROM club_info ORDER BY id DESC LIMIT 1"""
    )
    info = cur.fetchone()
    if info and ((info.get("city") or "").strip() or (info.get("address") or "").strip()):
        _sync_primary_club_venue(
            cur,
            name=info.get("name") or "Padel Club",
            city=(info.get("city") or "").strip(),
            address=(info.get("address") or "").strip(),
            phone=(info.get("contact_phone") or "").strip(),
        )
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
    telegram_username: str = None,
):
    conn = get_connection()
    cur = conn.cursor()
    uname = (telegram_username or "").lstrip("@").strip() or None
    cur.execute(
        """INSERT INTO users
           (telegram_id, name, phone, level, age, city, has_inventory, needs_rules,
            telegram_username)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (telegram_id, name, phone, level, age, city, has_inventory, needs_rules, uname),
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
    telegram_username: str = None,
):
    conn = get_connection()
    cur = conn.cursor()
    uname = (telegram_username or "").lstrip("@").strip() or None
    cur.execute(
        """UPDATE users
           SET name = %s, phone = %s, level = %s,
               age = %s, city = COALESCE(%s, city),
               has_inventory = %s, needs_rules = %s,
               telegram_username = COALESCE(%s, telegram_username)
           WHERE telegram_id = %s RETURNING *""",
        (name, phone, level, age, city, has_inventory, needs_rules, uname, telegram_id),
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
    cur.execute(f"""
        SELECT * FROM games
        WHERE (game_date + game_time) >= {_LOCAL_NOW_EXPR}
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


# Момент окончания игры = начало + длительность (по умолчанию 90 минут,
# если не указана) — используется и чтобы убрать завершившиеся игры из
# списка, и чтобы посчитать "идёт сейчас".
_GAME_END_EXPR = (
    "(g.game_date + g.game_time + (COALESCE(g.duration_minutes, 90) || ' minutes')::interval)"
)


def get_games_paginated(
    page: int = 1, per_page: int = 20, level: str = "",
    date_from=None, date_to=None, time_from=None, time_to=None, city: str = "",
    sort_order: str = "asc", fullness: str = "", show_past: bool = False,
    event_type: str = "game", coach_id=None,
):
    """Список игр/тренировок с пагинацией и фильтрами.

    sort_order: asc/desc по дате; coach_asc/coach_desc — по имени тренера
    (для тренировок). coach_id — фильтр по тренеру."""
    page = max(1, page)
    offset = (page - 1) * per_page

    where_clause = "WHERE 1=1"
    params: list = []
    if event_type:
        where_clause += " AND COALESCE(g.event_type, 'game') = %s"
        params.append(event_type)
    if level:
        where_clause += " AND g.level = %s"
        params.append(level)
    if date_from:
        where_clause += " AND g.game_date >= %s"
        params.append(date_from)
    if date_to:
        where_clause += " AND g.game_date <= %s"
        params.append(date_to)
    if time_from:
        where_clause += " AND g.game_time >= %s"
        params.append(time_from)
    if time_to:
        where_clause += " AND g.game_time <= %s"
        params.append(time_to)
    if city:
        where_clause += " AND g.city = %s"
        params.append(city)
    if coach_id:
        where_clause += " AND g.coach_id = %s"
        params.append(int(coach_id))

    # Пустая = нет броней через бота и нет ручных мест.
    _empty_expr = "(COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0)) = 0"
    _started_expr = f"(g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}"
    _ended_expr = f"{_GAME_END_EXPR} < {_LOCAL_NOW_EXPR}"

    if show_past:
        # Только прошедшие: слот закончился, пустая начавшаяся, или недобор.
        where_clause += f"""
          AND (
                {_ended_expr}
                OR ({_started_expr} AND {_empty_expr})
                OR COALESCE(g.underfill_cancelled, FALSE) = TRUE
              )
        """
    else:
        # Актуальные: не отменены по недобору, слот не закончился,
        # не «пустая начавшаяся».
        where_clause += f"""
          AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
          AND {_GAME_END_EXPR} >= {_LOCAL_NOW_EXPR}
          AND NOT ({_started_expr} AND {_empty_expr})
        """

    # Занятость = реальные бронирования + booked_places (мимо бота).
    # «Сначала ближайшие» / «дальние» для прошедших инвертируются:
    # ближайшие прошедшие = самые недавние (DESC), дальние = самые старые (ASC).
    if sort_order == "coach_asc":
        order_sql = "co.name ASC NULLS LAST, g.game_date ASC, g.game_time ASC"
    elif sort_order == "coach_desc":
        order_sql = "co.name DESC NULLS LAST, g.game_date DESC, g.game_time DESC"
    elif sort_order == "desc":
        order_sql = (
            "g.game_date ASC, g.game_time ASC"
            if show_past else
            "g.game_date DESC, g.game_time DESC"
        )
    else:
        order_sql = (
            "g.game_date DESC, g.game_time DESC"
            if show_past else
            "g.game_date ASC, g.game_time ASC"
        )

    if fullness == "full":
        fullness_clause = " AND (COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0)) >= g.total_slots"
    elif fullness == "available":
        fullness_clause = " AND (COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0)) < g.total_slots"
    else:
        fullness_clause = ""

    conn = get_connection()
    cur = conn.cursor()

    # total_count = COUNT(*) OVER() в том же запросе — см. _paginated_from_window.
    cur.execute(
        f"""
        SELECT g.*,
               c.name AS club_name,
               co.name AS coach_name,
               co.emoji AS coach_emoji,
               COALESCE(bk.taken, 0) AS taken,
               COALESCE(pm.collected, 0) AS collected,
               (
                   {_LOCAL_NOW_EXPR} >= (g.game_date + g.game_time)
                   AND {_LOCAL_NOW_EXPR} < {_GAME_END_EXPR}
                   AND (COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0)) > 0
               ) AS is_live,
               COUNT(*) OVER() AS total_count
        FROM games g
        LEFT JOIN clubs c ON c.id = g.club_id
        LEFT JOIN coaches co ON co.id = g.coach_id
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
        {where_clause}{fullness_clause}
        ORDER BY {order_sql}
        LIMIT %s OFFSET %s
        """,
        params + [per_page, offset],
    )
    games = cur.fetchall()
    result = _paginated_from_window(
        cur, games, page, per_page,
        f"""
        SELECT COUNT(*) AS cnt
        FROM games g
        LEFT JOIN (
            SELECT game_id, SUM(slots_count) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        {where_clause}{fullness_clause}
        """,
        params,
    )
    cur.close()
    conn.close()

    return result


def get_all_games_with_stats():
    """Обычные игры (не тренировки) с занятыми местами/оплатами одним запросом.
    Используется для Excel-отчёта со вкладки «Игры»."""
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
        WHERE COALESCE(g.event_type, 'game') = 'game'
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


def get_distinct_game_cities():
    """Список городов, встречающихся в играх — источник значений для
    выпадающего фильтра «место» на /games (только реально существующие в БД
    значения, а не статический список)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT city FROM games WHERE city IS NOT NULL AND city != '' ORDER BY city"
    )
    cities = [row["city"] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return cities


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
    booked_places=0, event_type="game", title=None, coach_id=None,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO games (
               game_date, game_time, location, price, total_slots,
               city, club_id, address, duration_minutes, level, booked_places,
               event_type, title, coach_id
           )
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (game_date, game_time, location, price, total_slots,
         city, club_id, address, duration_minutes, level, booked_places,
         event_type or "game", title, coach_id),
    )
    game = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return game


def update_game(
    game_id, game_date, game_time, location, price, total_slots,
    city=None, club_id=None, address=None, duration_minutes=90, level=None,
    booked_places=None, event_type=None, title=None, coach_id=None,
):
    """booked_places=None означает "не менять" — используется на случай,
    если update_game когда-нибудь вызовут без этого параметра (обратная
    совместимость); из формы CRM всегда приходит явное число."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE games
           SET game_date = %s, game_time = %s, location = %s,
               price = %s, total_slots = %s,
               city = %s, club_id = %s, address = %s,
               duration_minutes = %s, level = %s,
               booked_places = COALESCE(%s, booked_places),
               event_type = COALESCE(%s, event_type),
               title = %s,
               coach_id = %s
           WHERE id = %s
           RETURNING *""",
        (game_date, game_time, location, price, total_slots,
         city, club_id, address, duration_minutes, level, booked_places,
         event_type, title, coach_id, game_id),
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
    cur.execute(f"""
        SELECT * FROM games
        WHERE reminder_sent = FALSE
          AND (game_date + game_time) BETWEEN {_LOCAL_NOW_EXPR} + INTERVAL '23 hours'
                                            AND {_LOCAL_NOW_EXPR} + INTERVAL '25 hours'
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
    + имя игрока за 1 round-trip (вместо отдельных get_booking_by_id +
    update_booking_status + запроса имени для журнала действий).
    old_status берётся из подзапроса на состояние ДО обновления — Postgres
    вычисляет его атомарно в рамках одного UPDATE, так что это безопасно."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bookings AS b
        SET status = %s
        FROM (
            SELECT bk.status, u.name AS user_name
            FROM bookings bk
            JOIN users u ON u.id = bk.user_id
            WHERE bk.id = %s
        ) AS old
        WHERE b.id = %s
        RETURNING b.*, old.status AS old_status, old.user_name AS user_name
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
               g.event_type, g.title,
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


def delete_old_bookings(days: int = 60) -> int:
    """Удаляет бронирования на игры, которые прошли больше `days` дней
    назад — критерий по дате самой ИГРЫ (games.game_date), а не по дате
    создания брони: старые записи копятся годами и не нужны для работы
    бота/CRM. Раз игра уже прошла больше `days` дней назад, у неё по
    определению не может быть "будущих" броней — поэтому это условие
    физически не может задеть недавние/будущие игры, а сама таблица games
    здесь не трогается вообще (игры не удаляются, даже если у них не
    осталось ни одной брони). payments удаляются автоматически каскадом
    (payments.booking_id ON DELETE CASCADE)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        DELETE FROM bookings b
        USING games g
        WHERE b.game_id = g.id
          AND g.game_date < ({_LOCAL_TODAY_EXPR} - %s * INTERVAL '1 day')
        """,
        (days,),
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return deleted


def delete_old_admin_logs(days: int = 60) -> int:
    """Удаляет записи журнала действий старше `days` дней по created_at."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM admin_logs WHERE created_at < NOW() - %s * INTERVAL '1 day'",
        (days,),
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return deleted


def get_game_details(game_id: int):
    """Полная информация об игре для карточки "Подробнее" в CRM: сама игра
    (с клубом, реальным числом занятых мест из bookings, собранными оплатами)
    плюс список участников с их статусом брони/оплаты. Используется
    api_game_details в app.py."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT g.*, c.name AS club_name,
               coach.name AS coach_name, coach.emoji AS coach_emoji,
               COALESCE(bk.taken, 0) AS taken,
               COALESCE(pm.collected, 0) AS collected,
               ({_LOCAL_NOW_EXPR} >= (g.game_date + g.game_time) AND {_LOCAL_NOW_EXPR} < {_GAME_END_EXPR}) AS is_live
        FROM games g
        LEFT JOIN clubs c ON c.id = g.club_id
        LEFT JOIN coaches coach ON coach.id = g.coach_id
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
        WHERE g.id = %s
        """,
        (game_id,),
    )
    game = cur.fetchone()
    if not game:
        cur.close()
        conn.close()
        return None

    cur.execute(
        """
        SELECT b.id AS booking_id, b.status AS booking_status, b.slots_count,
               b.created_at AS booked_at,
               u.name AS user_name, u.phone AS user_phone, u.level AS user_level,
               u.telegram_id,
               p.status AS payment_status, p.amount AS payment_amount, p.method AS payment_method
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        LEFT JOIN LATERAL (
            SELECT status, amount, method
            FROM payments
            WHERE booking_id = b.id
            ORDER BY id DESC
            LIMIT 1
        ) p ON TRUE
        WHERE b.game_id = %s
        ORDER BY b.created_at
        """,
        (game_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    participants = []
    cancelled = []
    for row in rows:
        item = dict(row)
        pay = item.get("payment_status")
        booking_status = item.get("booking_status")
        is_cancelled = (
            booking_status == "отменена"
            or pay in ("возврат", "возврат оформлен")
        )
        if is_cancelled:
            cancelled.append(item)
        elif pay == "подтверждена" and booking_status != "отменена":
            participants.append(item)
    return {
        "game": dict(game),
        "participants": participants,
        "cancelled": cancelled,
    }


def get_participants_for_game(game_id: int):
    """Список игроков, записанных на конкретную игру (для напоминаний)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.telegram_id, u.name, u.telegram_username,
               b.id AS booking_id, b.slots_count, b.user_id
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        WHERE b.game_id = %s AND b.status != 'отменена'
        ORDER BY b.created_at, b.id
    """, (game_id,))
    participants = cur.fetchall()
    cur.close()
    conn.close()
    return participants


def get_user_statistics(user_id: int) -> dict:
    """Персональная статистика — тот же смысл «посещения», что в CRM и боте:
    незакрытая запись на уже начавшуюся игру/тренировку."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE b.status = 'отменена') AS cancelled,
            COUNT(*) FILTER (
                WHERE COALESCE(g.underfill_cancelled, FALSE) = FALSE
                  AND (g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}
            ) AS past_total,
            COUNT(*) FILTER (
                WHERE b.status != 'отменена'
                  AND COALESCE(b.no_show, FALSE) = FALSE
                  AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
                  AND (g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}
            ) AS attended,
            COUNT(*) FILTER (
                WHERE COALESCE(b.no_show, FALSE) = TRUE
                  AND b.status != 'отменена'
                  AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
                  AND (g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}
            ) AS no_shows,
            (
                SELECT COUNT(DISTINCT b2.id)
                FROM bookings b2
                JOIN payments p ON p.booking_id = b2.id
                WHERE b2.user_id = %s AND p.status = 'подтверждена'
            ) AS paid
        FROM bookings b
        LEFT JOIN games g ON g.id = b.game_id
        WHERE b.user_id = %s
        """,
        (user_id, user_id),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    total = int(row["total"] or 0)
    cancelled = int(row["cancelled"] or 0)
    attended = int(row["attended"] or 0)
    no_shows = int(row["no_shows"] or 0)
    past_total = int(row["past_total"] or 0)
    paid = int(row["paid"] or 0)

    attendance_rate = round(attended / past_total * 100) if past_total > 0 else 0
    hours_played = round(attended * 1.5, 1)

    return {
        "total": total,
        "paid": paid,
        "attended": attended,
        "no_shows": no_shows,
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

    query += " AND NOT (b.status = 'отменена' AND p.status = 'ожидает')"
    query += " ORDER BY p.created_at DESC"

    cur.execute(query, params)
    payments = cur.fetchall()
    cur.close()
    conn.close()
    return payments


def delete_pending_payments_for_booking(booking_id: int) -> int:
    """Удаляет неоплаченные платежи «ожидает» по заявке.
    Строки, где игрок уже сообщил об оплате (player_notified_at), не трогаем —
    их переводит в «возврат» mark_confirmed_payments_refund_for_booking."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM payments
        WHERE booking_id = %s
          AND status = 'ожидает'
          AND player_notified_at IS NULL
        """,
        (booking_id,),
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return deleted


def mark_confirmed_payments_refund_for_booking(booking_id: int) -> int:
    """При отмене заявки админом — подтверждённые и уже заявленные игроком
    оплаты («Я оплатил») → «возврат». Бейдж «+N» через admin_attention_at."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE payments
           SET status = 'возврат',
               admin_attention_at = NOW()
         WHERE booking_id = %s
           AND (
               status = 'подтверждена'
               OR (status = 'ожидает' AND player_notified_at IS NOT NULL)
           )
        """,
        (booking_id,),
    )
    updated = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    if updated:
        clear_badge_cache()
    return updated


def booking_has_notified_pending_payment(booking_id: int) -> bool:
    """Есть ли оплата, которую игрок уже отправил, но админ ещё не подтвердил."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM payments
        WHERE booking_id = %s
          AND status = 'ожидает'
          AND player_notified_at IS NOT NULL
        LIMIT 1
        """,
        (booking_id,),
    )
    found = cur.fetchone() is not None
    cur.close()
    conn.close()
    return found


def get_payments_paginated(search: str = "", status: str = "", page: int = 1, per_page: int = 20):
    """Оплаты с фильтрацией и пагинацией — для CRM.
    Сверху всегда «ожидает» и «возврат» (нужно действие админа), затем остальные."""
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

    # Заявки, отменённые до оплаты: платёж со статусом «ожидает» не показываем
    # (при отмене бот/CRM его удаляют, фильтр — страховка для старых записей).
    where_clause += " AND NOT (b.status = 'отменена' AND p.status = 'ожидает')"

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT p.*, u.name AS user_name, b.game_id, g.game_date, g.game_time, g.location,
               COALESCE(g.event_type, 'game') AS event_type, g.title,
               COUNT(*) OVER() AS total_count
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        {where_clause}
        ORDER BY
            CASE
                WHEN p.status IN ('ожидает', 'возврат') THEN 0
                ELSE 1
            END,
            COALESCE(p.admin_attention_at, p.created_at) DESC NULLS LAST,
            p.id DESC
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
    """Атомарно подтверждает платёж «ожидает» у активной заявки и ставит
    статус заявки «подтверждена». Возвращает обновлённую строку оплаты или None."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE payments p
               SET status = 'подтверждена',
                   player_notified_at = COALESCE(p.player_notified_at, NOW())
              FROM bookings b
             WHERE p.id = %s
               AND p.booking_id = b.id
               AND p.status = 'ожидает'
               AND b.status != 'отменена'
             RETURNING p.*, b.id AS booking_id, b.status AS booking_status_before
            """,
            (payment_id,),
        )
        updated = cur.fetchone()
        if not updated:
            conn.rollback()
            return None
        booking_id = updated["booking_id"]
        # Заявка тоже становится подтверждённой (если ещё «новая»/иная, кроме отмены).
        cur.execute(
            """
            UPDATE bookings
               SET status = 'подтверждена'
             WHERE id = %s
               AND status != 'отменена'
               AND status != 'посещена'
            """,
            (booking_id,),
        )
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_payment_by_id(payment_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM payments WHERE id = %s", (payment_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def attach_provider_payment(
    payment_id: int,
    provider_payment_id: str,
    confirmation_url: str,
    method: str = "yookassa",
):
    """Сохраняет id и ссылку ЮKassa на локальном платеже."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE payments
           SET provider_payment_id = %s,
               confirmation_url = %s,
               method = COALESCE(method, %s)
           WHERE id = %s AND status = 'ожидает'
           RETURNING *""",
        (provider_payment_id, confirmation_url, method, payment_id),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def get_payment_by_provider_id(provider_payment_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM payments WHERE provider_payment_id = %s",
        (provider_payment_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def set_payment_method_sync(payment_id: int, method: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE payments SET method = %s WHERE id = %s",
        (method, payment_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def mark_payment_notified_sync(payment_id: int):
    """Игрок оплатил / сообщил об оплате — ждём подтверждения админа в CRM.
    Заполняет player_notified_at и admin_attention_at → бейдж «+N»."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE payments
              SET player_notified_at = NOW(),
                  admin_attention_at = NOW()
            WHERE id = %s AND status = 'ожидает' AND player_notified_at IS NULL
            RETURNING *""",
        (payment_id,),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if row is not None:
        clear_badge_cache()
    return row


def register_provider_payment_awaiting_admin(
    provider_payment_id: str,
    local_payment_id: int = None,
    expected_amount=None,
    payment_method: str = None,
):
    """Webhook провайдера: деньги пришли, статус остаётся «ожидает»,
    выставляем player_notified_at — админ подтвердит в CRM.

    Вызывать только после проверки платежа через API провайдера.
    Привязка по local_payment_id разрешена лишь если у строки ещё нет
    другого provider_payment_id (нельзя перезаписать чужим id).

    Возвращает {"status": "ok"|"already"|"already_notified"|"not_found"|
                "mismatch"|"forbidden"|"cancelled", "payment": row|None}.
    """
    if not provider_payment_id or expected_amount is None:
        return {"status": "mismatch", "payment": None}

    payment = get_payment_by_provider_id(provider_payment_id)
    if payment is None and local_payment_id:
        local = get_payment_by_id(local_payment_id)
        if local is None:
            return {"status": "not_found", "payment": None}
        if local.get("status") != "ожидает":
            return {"status": "forbidden", "payment": local}
        existing_provider = (local.get("provider_payment_id") or "").strip()
        if existing_provider and existing_provider != str(provider_payment_id):
            # Не даём webhook подменить уже привязанный provider id.
            return {"status": "forbidden", "payment": local}
        if not existing_provider:
            attach_provider_payment(
                int(local["id"]),
                provider_payment_id,
                local.get("confirmation_url") or "",
                method=payment_method or "yookassa",
            )
        payment = get_payment_by_id(int(local["id"]))

    if payment is None:
        return {"status": "not_found", "payment": None}
    if payment.get("status") == "подтверждена":
        return {"status": "already", "payment": payment}
    if payment.get("status") != "ожидает":
        return {"status": "forbidden", "payment": payment}
    if abs(float(payment["amount"]) - float(expected_amount)) > 0.009:
        return {"status": "mismatch", "payment": payment}

    # Не отмечаем оплату по уже отменённой заявке.
    booking = get_booking_by_id(int(payment["booking_id"])) if payment.get("booking_id") else None
    if booking and booking.get("status") == "отменена":
        return {"status": "cancelled", "payment": payment}

    if payment_method:
        set_payment_method_sync(int(payment["id"]), payment_method)

    if payment.get("player_notified_at") is not None:
        return {"status": "already_notified", "payment": get_payment_by_id(int(payment["id"]))}

    updated = mark_payment_notified_sync(int(payment["id"]))
    if not updated:
        again = get_payment_by_id(int(payment["id"]))
        if again and again.get("player_notified_at") is not None:
            return {"status": "already_notified", "payment": again}
        return {"status": "forbidden", "payment": payment}
    return {"status": "ok", "payment": updated}


def confirm_payment_from_yookassa(
    provider_payment_id: str,
    local_payment_id: int = None,
    expected_amount=None,
    payment_method: str = None,
):
    """Обратная совместимость: теперь только регистрирует оплату для админа."""
    return register_provider_payment_awaiting_admin(
        provider_payment_id=provider_payment_id,
        local_payment_id=local_payment_id,
        expected_amount=expected_amount,
        payment_method=payment_method,
    )


def confirm_refund(payment_id: int):
    """Финальный шаг возврата: админ вручную подтверждает, что деньги
    реально отправлены игроку (перевод/наличные и т.п. — сам процесс
    возврата вне системы, здесь только фиксация факта). Условие
    `status = 'возврат'` в WHERE — защита от повторного/гонки нажатия:
    если платёж уже не в статусе 'возврат' (например, админ уже кликнул
    в соседней вкладке), UPDATE не найдёт строку и вернёт None."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE payments SET status = 'возврат оформлен' WHERE id = %s AND status = 'возврат' RETURNING *",
        (payment_id,),
    )
    updated = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return updated


def get_payment_notification_context(payment_id: int):
    """Данные, нужные, чтобы уведомить игрока в Telegram сразу после того,
    как админ подтвердил его оплату в CRM: telegram_id игрока + когда/где
    игра. Один запрос с джойнами вместо нескольких точечных выборок."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT p.id AS payment_id, p.amount, p.status, p.method,
               u.id AS user_id, u.telegram_id, u.name AS user_name,
               u.phone AS user_phone, u.telegram_username, u.level AS user_level,
               b.id AS booking_id, b.slots_count, b.status AS booking_status,
               g.id AS game_id, g.game_date, g.game_time, g.location,
               g.event_type, g.title, g.price,
               (
                   SELECT COUNT(*)::int FROM payments p2
                   WHERE p2.booking_id = b.id
                     AND p2.status = 'подтверждена'
                     AND p2.id != p.id
               ) AS prior_confirmed_count,
               ROUND((
                   SELECT COUNT(*)::numeric * 1.5
                   FROM bookings bx
                   JOIN games gx ON gx.id = bx.game_id
                   WHERE bx.user_id = u.id
                     AND bx.status != 'отменена'
                     AND COALESCE(bx.no_show, FALSE) = FALSE
                     AND COALESCE(gx.underfill_cancelled, FALSE) = FALSE
                     AND (gx.game_date + gx.game_time) <= {_LOCAL_NOW_EXPR}
               ), 1) AS hours_played
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE p.id = %s
        """,
        (payment_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_paid_game_mates(game_id: int, exclude_user_id: int = None):
    """Игроки с подтверждённой оплатой на игре (кроме exclude_user_id)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT u.telegram_id, u.name, u.telegram_username, u.level,
               b.id AS booking_id, b.slots_count, b.user_id,
               ROUND((
                   SELECT COUNT(*)::numeric * 1.5
                   FROM bookings bx
                   JOIN games gx ON gx.id = bx.game_id
                   WHERE bx.user_id = u.id
                     AND bx.status != 'отменена'
                     AND COALESCE(bx.no_show, FALSE) = FALSE
                     AND COALESCE(gx.underfill_cancelled, FALSE) = FALSE
                     AND (gx.game_date + gx.game_time) <= {_LOCAL_NOW_EXPR}
               ), 1) AS hours_played
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        WHERE b.game_id = %s
          AND b.status != 'отменена'
          AND (%s::int IS NULL OR b.user_id != %s)
          AND EXISTS (
              SELECT 1 FROM payments p
              WHERE p.booking_id = b.id AND p.status = 'подтверждена'
          )
        ORDER BY b.created_at, b.id
        """,
        (game_id, exclude_user_id, exclude_user_id),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


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


def mark_booking_no_show(booking_id: int, no_show: bool = True):
    """Отметка «Не был» в CRM. Возвращает бронь + данные для уведомления."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bookings AS b
        SET no_show = %s
        FROM users u, games g
        WHERE b.id = %s
          AND u.id = b.user_id
          AND g.id = b.game_id
          AND b.status != 'отменена'
        RETURNING b.*,
                  u.name AS user_name,
                  u.telegram_id,
                  g.game_date, g.game_time, g.location,
                  COALESCE(g.event_type, 'game') AS event_type, g.title
        """,
        (bool(no_show), booking_id),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def mark_booking_visited_and_get(booking_id: int):
    """Как mark_booking_visited, но сразу возвращает обновлённую строку и имя
    игрока (для человекочитаемой записи в журнале действий) — без отдельного
    запроса имени."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bookings AS b
        SET status = 'посещена'
        FROM (
            SELECT u.name AS user_name
            FROM bookings bk
            JOIN users u ON u.id = bk.user_id
            WHERE bk.id = %s
        ) AS info
        WHERE b.id = %s
        RETURNING b.*, info.user_name AS user_name
        """,
        (booking_id, booking_id),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


# Игроки прошедших/начавшихся игр и тренировок: бронь не отменена и время
# старта уже прошло (по Москве). Раньше вкладка «Посещения» смотрела только
# status='посещена', который почти никто не ставил вручную — список был пустым.
_VISITS_WHERE = f"""
    b.status != 'отменена'
    AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
    AND (g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}
"""


def get_all_visits():
    """Все сыгравшие игроки с данными об играх и тренировках (для отчётов)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT b.*, u.name AS user_name, u.phone AS user_phone,
               g.id AS game_id, g.game_date, g.game_time, g.location,
               COALESCE(g.event_type, 'game') AS event_type, g.title
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE {_VISITS_WHERE}
        ORDER BY g.game_date DESC, g.game_time DESC, u.name
    """)
    visits = cur.fetchall()
    cur.close()
    conn.close()
    return visits


def get_visits_paginated(page: int = 1, per_page: int = 20):
    """Сыгравшие игроки с пагинацией — вкладка CRM «Посещения».
    Показывает имя, телефон, игру/тренировку и дату для броней на уже
    начавшиеся / прошедшие события (не отменённые)."""
    page = max(1, page)
    offset = (page - 1) * per_page

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT b.*, u.name AS user_name, u.phone AS user_phone, u.telegram_id,
               g.id AS game_id, g.game_date, g.game_time, g.location,
               COALESCE(g.event_type, 'game') AS event_type, g.title,
               COUNT(*) OVER() AS total_count
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        JOIN games g ON g.id = b.game_id
        WHERE {_VISITS_WHERE}
        ORDER BY g.game_date DESC, g.game_time DESC, u.name
        LIMIT %s OFFSET %s
        """,
        (per_page, offset),
    )
    visits = cur.fetchall()
    result = _paginated_from_window(
        cur, visits, page, per_page,
        f"""
        SELECT COUNT(*) AS cnt
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        WHERE {_VISITS_WHERE}
        """,
    )
    cur.close()
    conn.close()

    return result


# ---------------------------------------------------------------------------
# CLUBS — клубы/площадки
# ---------------------------------------------------------------------------

def create_club(name: str, city: str, address: str, phone: str, description: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO clubs (name, city, address, phone, description)
           VALUES (%s, %s, %s, %s, %s) RETURNING *""",
        (name, city, address, phone, description),
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


def update_club(
    club_id: int, name: str, city: str, address: str, phone: str, description: str = "",
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE clubs
           SET name = %s, city = %s, address = %s, phone = %s, description = %s
           WHERE id = %s""",
        (name, city, address, phone, description, club_id),
    )
    conn.commit()
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# COACHES — тренеры
# ---------------------------------------------------------------------------

def get_all_coaches(active_only: bool = False):
    conn = get_connection()
    cur = conn.cursor()
    if active_only:
        cur.execute(
            "SELECT * FROM coaches WHERE is_active = TRUE "
            "ORDER BY sort_order, name"
        )
    else:
        cur.execute("SELECT * FROM coaches ORDER BY sort_order, name")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_coaches_paginated(page: int = 1, per_page: int = 20):
    page = max(1, page)
    offset = (page - 1) * per_page
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT *, COUNT(*) OVER() AS total_count
           FROM coaches
           ORDER BY sort_order, name
           LIMIT %s OFFSET %s""",
        (per_page, offset),
    )
    rows = cur.fetchall()
    result = _paginated_from_window(
        cur, rows, page, per_page, "SELECT COUNT(*) AS cnt FROM coaches",
    )
    cur.close()
    conn.close()
    return result


def get_coach_by_id(coach_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM coaches WHERE id = %s", (coach_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def create_coach(
    name: str,
    phone: str = "",
    telegram_username: str = "",
    experience: str = "",
    specialization: str = "",
    achievements: str = "",
    emoji: str = "🧑‍🏫",
    is_active: bool = True,
    sort_order: int = 0,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO coaches (
               name, phone, telegram_username, experience,
               specialization, achievements, emoji, is_active, sort_order
           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (
            name, phone or "", (telegram_username or "").lstrip("@"),
            experience or "", specialization or "", achievements or "",
            emoji or "🧑‍🏫", bool(is_active), int(sort_order or 0),
        ),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def update_coach(
    coach_id: int,
    name: str,
    phone: str = "",
    telegram_username: str = "",
    experience: str = "",
    specialization: str = "",
    achievements: str = "",
    emoji: str = "🧑‍🏫",
    is_active: bool = True,
    sort_order: int = 0,
):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """UPDATE coaches SET
               name = %s, phone = %s, telegram_username = %s,
               experience = %s, specialization = %s, achievements = %s,
               emoji = %s, is_active = %s, sort_order = %s
           WHERE id = %s RETURNING *""",
        (
            name, phone or "", (telegram_username or "").lstrip("@"),
            experience or "", specialization or "", achievements or "",
            emoji or "🧑‍🏫", bool(is_active), int(sort_order or 0), coach_id,
        ),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def delete_coach(coach_id: int):
    """Мягкое удаление — скрываем тренера; тренировки с ним остаются в истории."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE coaches SET is_active = FALSE WHERE id = %s RETURNING *",
        (coach_id,),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


def permanently_delete_coach(coach_id: int):
    """Полное удаление тренера. У тренировок coach_id обнуляется (история слотов остаётся)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE games SET coach_id = NULL WHERE coach_id = %s",
        (coach_id,),
    )
    cur.execute(
        "DELETE FROM coaches WHERE id = %s RETURNING *",
        (coach_id,),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return row


# ---------------------------------------------------------------------------
# ADMIN LOGS — журнал действий администратора
# ---------------------------------------------------------------------------

def log_action(
    action: str,
    entity_type: str = None,
    entity_id: int = None,
    description: str = None,
    old_value: str = None,
    new_value: str = None,
    details: str = None,
):
    """Записывает действие администратора в журнал (admin_logs).

    description — человекочитаемый текст на русском ("Игра №9 создана: ...",
    "Статус оплаты для брони №5 изменён с «ожидает» на «подтверждена»") —
    именно он показывается в /logs. old_value/new_value оставлены для
    редких случаев, когда нужно сохранить простое старое/новое значение
    (например статус) отдельно от текста; details — свободный комментарий."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO admin_logs (action, entity_type, entity_id, description, old_value, new_value, details)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (action, entity_type, entity_id, description, old_value, new_value, details),
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


def get_logs_paginated(page: int = 1, per_page: int = 20, entity_type: str = ""):
    """Журнал действий с пагинацией — для CRM. entity_type — опциональный
    фильтр по типу сущности (game/booking/payment/club/club_info)."""
    page = max(1, page)
    offset = (page - 1) * per_page

    where_clause = "WHERE 1=1"
    params: list = []
    if entity_type:
        where_clause += " AND entity_type = %s"
        params.append(entity_type)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""SELECT *, COUNT(*) OVER() AS total_count FROM admin_logs
            {where_clause} ORDER BY created_at DESC LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    logs = cur.fetchall()
    result = _paginated_from_window(
        cur, logs, page, per_page, f"SELECT COUNT(*) AS cnt FROM admin_logs {where_clause}", params,
    )
    cur.close()
    conn.close()

    return result


def get_distinct_log_entity_types():
    """Список entity_type, реально встречающихся в журнале — источник
    значений для выпадающего фильтра на /logs."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT entity_type FROM admin_logs WHERE entity_type IS NOT NULL ORDER BY entity_type"
    )
    types = [row["entity_type"] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return types


def get_dashboard_summary() -> dict:
    """Все 6 счётчиков главной страницы CRM одним запросом (1 round-trip к
    БД вместо 6 — раньше index() дёргал count_games/count_active_bookings/
    count_pending_payments/count_visits/count_clubs/count_logs отдельно,
    и это было самой медленной страницей CRM)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM games
             WHERE COALESCE(underfill_cancelled, FALSE) = FALSE
               AND COALESCE(event_type, 'game') = 'game'
               AND (game_date + game_time) >= {_LOCAL_NOW_EXPR}) AS games,
            (SELECT COUNT(*) FROM bookings WHERE status != 'отменена') AS bookings,
            (SELECT COUNT(*) FROM payments p
             JOIN bookings b ON b.id = p.booking_id
             WHERE p.status = 'ожидает' AND b.status != 'отменена') AS pending_payments,
            (SELECT COUNT(*) FROM bookings b
             JOIN games g ON g.id = b.game_id
             WHERE b.status != 'отменена'
               AND COALESCE(b.no_show, FALSE) = FALSE
               AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
               AND (g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}) AS visits,
            (SELECT COUNT(*) FROM clubs) AS clubs,
            (SELECT COUNT(*) FROM admin_logs) AS logs
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)


def get_latest_activity_marker() -> dict:
    """Лёгкий «маркер свежести» для заявок, оплат и отзывов: max(id) + общее
    количество строк по bookings/payments/reviews. Используется CRM-страницами
    для поллинга — сравниваем с тем, что было при рендере, и если что-то
    изменилось (новая заявка/оплата/отзыв или правка другим админом),
    подсказываем обновить страницу (bookings.html/payments.html), а также
    используется для сброса счётчика-бейджа "+N" в шапке меню при заходе в
    раздел (см. bookings_list/payments_list/reviews_list в app.py). Один
    простой запрос, не нагружает БД при частом опросе (индексы по id есть по
    умолчанию — это PK)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (SELECT COALESCE(MAX(id), 0) FROM bookings) AS max_booking_id,
            (SELECT COALESCE(MAX(id), 0) FROM payments) AS max_payment_id,
            (SELECT MAX(admin_attention_at) FROM payments) AS last_payment_notified_at,
            (SELECT COALESCE(MAX(id), 0) FROM reviews) AS max_review_id,
            (SELECT COUNT(*) FROM bookings) AS bookings_count,
            (SELECT COUNT(*) FROM payments) AS payments_count,
            (SELECT COUNT(*) FROM reviews) AS reviews_count
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)


# Платежи, которые должны подсвечивать бейдж «Оплаты»: ждут подтверждения
# оплаты или ждут оформления возврата.
_PAYMENT_BADGE_WHERE = """
    admin_attention_at IS NOT NULL
    AND admin_attention_at > COALESCE(%s::timestamp, 'epoch'::timestamp)
    AND (
        (status = 'ожидает' AND player_notified_at IS NOT NULL)
        OR status = 'возврат'
    )
"""


# Заявки в бейдже — только «новая»: после подтверждения оплаты заявка
# становится «подтверждена», и +N у «Заявки» гаснет без захода в раздел.
_BOOKING_BADGE_WHERE = "id > %s AND status = 'новая'"


def count_new_since(last_booking_id: int, last_payment_notified_at, last_review_id: int) -> dict:
    """Сколько новых заявок/оплат/отзывов после last_* — бейджи "+N" в шапке.

    new_bookings считает только статус «новая» (не все id > watermark):
    подтверждение оплаты автоматически снимает бейдж у «Заявки»."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            (SELECT COUNT(*) FROM bookings WHERE {_BOOKING_BADGE_WHERE}) AS new_bookings,
            (SELECT COUNT(*) FROM payments
                WHERE {_PAYMENT_BADGE_WHERE}) AS new_payments,
            (SELECT COUNT(*) FROM reviews WHERE id > %s) AS new_reviews
        """,
        (last_booking_id, last_payment_notified_at, last_review_id),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)

_badge_cache_lock = threading.Lock()
_badge_cache = {}  # key -> (monotonic_ts, counts_dict)
_BADGE_CACHE_TTL = 10.0


def clear_badge_cache() -> None:
    """Сброс in-memory кэша бейджей — вызывается после player_notified_at,
    чтобы "+N" у «Оплаты» появился на следующем /api/activity без ожидания TTL."""
    with _badge_cache_lock:
        _badge_cache.clear()

_dashboard_cache_lock = threading.Lock()
_dashboard_cache = None  # (monotonic_ts, summary_dict)
_DASHBOARD_CACHE_TTL = 5.0


def peek_badge_counts(last_booking_id: int, last_payment_notified_at, last_review_id: int):
    """Только in-memory кэш бейджей — без обращения к БД.

    Нужен шапке CRM: иначе каждый клик по меню платит ~1с round-trip к Neon
    сверх запроса самой страницы. Промах → None, бейджи подтянет поллинг
    /api/activity."""
    key = (last_booking_id, last_payment_notified_at, last_review_id)
    now = time.monotonic()
    with _badge_cache_lock:
        hit = _badge_cache.get(key)
        if hit and (now - hit[0]) < _BADGE_CACHE_TTL:
            return hit[1]
    return None


def count_new_since_cached(last_booking_id: int, last_payment_notified_at, last_review_id: int) -> dict:
    """Тот же count_new_since, но с in-memory TTL: /api/activity и редкие
    места, где бейджи всё же нужно посчитать сразу."""
    key = (last_booking_id, last_payment_notified_at, last_review_id)
    now = time.monotonic()
    with _badge_cache_lock:
        hit = _badge_cache.get(key)
        if hit and (now - hit[0]) < _BADGE_CACHE_TTL:
            return hit[1]
    counts = count_new_since(last_booking_id, last_payment_notified_at, last_review_id)
    with _badge_cache_lock:
        _badge_cache[key] = (now, counts)
    return counts


def get_dashboard_summary_cached() -> dict:
    """Кэш главной CRM на несколько секунд — повторные заходы/клики
    «Главная» не ждут ещё один ~1с round-trip к Neon."""
    global _dashboard_cache
    now = time.monotonic()
    with _dashboard_cache_lock:
        if _dashboard_cache and (now - _dashboard_cache[0]) < _DASHBOARD_CACHE_TTL:
            return _dashboard_cache[1]
    summary = get_dashboard_summary()
    with _dashboard_cache_lock:
        _dashboard_cache = (time.monotonic(), summary)
    return summary


def get_activity_snapshot(last_booking_id: int, last_payment_notified_at, last_review_id: int) -> dict:
    """Маркер свежести + new_* бейджи одним round-trip — для /api/activity.
    Раньше было два подряд запроса (get_latest_activity_marker +
    count_new_since), и на удалённом Neon это давало ~2с на каждый опрос."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            (SELECT COALESCE(MAX(id), 0) FROM bookings) AS max_booking_id,
            (SELECT COALESCE(MAX(id), 0) FROM payments) AS max_payment_id,
            (SELECT MAX(admin_attention_at) FROM payments) AS last_payment_notified_at,
            (SELECT COALESCE(MAX(id), 0) FROM reviews) AS max_review_id,
            (SELECT COUNT(*) FROM bookings) AS bookings_count,
            (SELECT COUNT(*) FROM payments) AS payments_count,
            (SELECT COUNT(*) FROM reviews) AS reviews_count,
            (SELECT COUNT(*) FROM bookings WHERE {_BOOKING_BADGE_WHERE}) AS new_bookings,
            (SELECT COUNT(*) FROM payments
                WHERE {_PAYMENT_BADGE_WHERE}) AS new_payments,
            (SELECT COUNT(*) FROM reviews WHERE id > %s) AS new_reviews
        """,
        (last_booking_id, last_payment_notified_at, last_review_id),
    )
    row = dict(cur.fetchone())
    cur.close()
    conn.close()
    # Обновляем кэш бейджей — следующий рендер шапки не пойдёт в БД снова.
    key = (last_booking_id, last_payment_notified_at, last_review_id)
    counts = {
        "new_bookings": row["new_bookings"],
        "new_payments": row["new_payments"],
        "new_reviews": row["new_reviews"],
    }
    with _badge_cache_lock:
        _badge_cache[key] = (time.monotonic(), counts)
    return row


def count_pending_payments() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS cnt FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        WHERE p.status = 'ожидает' AND b.status != 'отменена'
        """
    )
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
    """Число фактических посещений (без неявок) — то же, что карточка
    «Посещений» на главной CRM."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        WHERE {_VISITS_WHERE}
          AND COALESCE(b.no_show, FALSE) = FALSE
        """
    )
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


def update_club_info(
    name: str,
    description: str,
    contact_phone: str,
    contact_email: str = "",
    admin_telegram_id=None,
    admin_telegram_username=None,
    city: str = "",
    address: str = "",
    bot_show: dict = None,
):
    """admin_telegram_id=None — не менять id; ''/0 — очистить.
    admin_telegram_username=None — не менять username; '' — очистить.
    city/address сохраняются в club_info и синхронизируются в clubs.
    bot_show — dict флагов видимости полей в боте (bot_show_name и т.д.)."""
    conn = get_connection()
    cur = conn.cursor()
    sets = [
        "name = %s",
        "description = %s",
        "contact_phone = %s",
        "contact_email = %s",
        "city = %s",
        "address = %s",
        "updated_at = NOW()",
    ]
    params = [name, description, contact_phone, contact_email, city or "", address or ""]
    if bot_show:
        for key in (
            "bot_show_name", "bot_show_city", "bot_show_address",
            "bot_show_description", "bot_show_phone", "bot_show_email",
            "bot_show_admin_username",
        ):
            if key in bot_show:
                sets.append(f"{key} = %s")
                params.append(bool(bot_show[key]))
    if admin_telegram_id is not None:
        tid = None
        if admin_telegram_id not in ("", 0, "0"):
            tid = int(admin_telegram_id)
        sets.append("admin_telegram_id = %s")
        params.append(tid)
    if admin_telegram_username is not None:
        uname = str(admin_telegram_username).lstrip("@").strip() or None
        sets.append("admin_telegram_username = %s")
        params.append(uname)
    cur.execute(
        f"""UPDATE club_info SET {', '.join(sets)}
           WHERE id = (SELECT id FROM club_info ORDER BY id DESC LIMIT 1)""",
        tuple(params),
    )
    _sync_primary_club_venue(cur, name=name, city=city or "", address=address or "", phone=contact_phone or "")
    conn.commit()
    cur.close()
    conn.close()


def _sync_primary_club_venue(cur, name: str, city: str, address: str, phone: str = "") -> None:
    """Одна площадка в clubs + перепривязка всех игр/тренировок на неё."""
    cur.execute("SELECT id FROM clubs ORDER BY id ASC LIMIT 1")
    row = cur.fetchone()
    if row:
        club_id = row["id"]
        cur.execute(
            """UPDATE clubs
               SET name = %s, city = %s, address = %s, phone = %s
               WHERE id = %s""",
            (name, city, address, phone, club_id),
        )
    else:
        cur.execute(
            """INSERT INTO clubs (name, city, address, phone, description)
               VALUES (%s, %s, %s, %s, %s) RETURNING id""",
            (name, city, address, phone, ""),
        )
        club_id = cur.fetchone()["id"]

    location = ", ".join(part for part in (name, city, address) if part)
    cur.execute(
        """UPDATE games
           SET club_id = %s,
               city = %s,
               address = %s,
               location = %s""",
        (club_id, city, address, location),
    )
    # В списке выбора остаётся только актуальный клуб.
    cur.execute("DELETE FROM clubs WHERE id != %s", (club_id,))


def set_club_admin_telegram_id(telegram_id: int, username: str = None) -> None:
    """Привязка Telegram ID админа из бота (/bindadmin) или CRM."""
    conn = get_connection()
    cur = conn.cursor()
    uname = str(username).lstrip("@").strip() if username else None
    uname = uname or None
    cur.execute(
        """UPDATE club_info
           SET admin_telegram_id = %s,
               admin_telegram_username = COALESCE(%s, admin_telegram_username),
               updated_at = NOW()
           WHERE id = (SELECT id FROM club_info ORDER BY id DESC LIMIT 1)""",
        (int(telegram_id), uname),
    )
    conn.commit()
    cur.close()
    conn.close()
