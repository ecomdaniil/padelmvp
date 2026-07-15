"""
database_async.py
------------------
Асинхронный слой доступа к PostgreSQL для Telegram-бота (bot.py), построенный
на asyncpg с пулом соединений. Это отдельный слой от database.py (который
использует синхронный psycopg2 и обслуживает CRM/app.py) — так CRM можно не
трогать, а бот при этом больше не блокирует event loop на каждом запросе к БД.

Здесь же реализованы:
- пул соединений (создаётся один раз на процесс бота);
- транзакционная (без race condition) запись на игру с блокировкой строки;
- проверка владельца записи при отмене (защита от IDOR);
- агрегирующие запросы вместо N+1 (список игр со свободными местами,
  статистика пользователя одним запросом вместо четырёх).

Все запросы параметризованы ($1, $2, ...) — никаких f-строк с пользовательским
вводом.
"""

import asyncio
import os
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


def _prepare_dsn(url: str):
    """asyncpg не всегда понимает sslmode= в DSN одинаково на всех версиях,
    поэтому вынимаем его явно и передаём отдельным параметром ssl=."""
    parts = urlsplit(url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    ssl_mode = None
    remaining = []
    for key, value in pairs:
        if key == "sslmode":
            ssl_mode = value
        else:
            remaining.append((key, value))
    clean_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(remaining), parts.fragment))
    ssl_option = "require" if ssl_mode in {"require", "verify-ca", "verify-full"} else None
    return clean_url, ssl_option


async def get_pool() -> asyncpg.Pool:
    """Возвращает общий пул соединений, создавая его при первом обращении."""
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is None:
            if not DATABASE_URL:
                raise RuntimeError("Не найден DATABASE_URL. Проверьте файл .env")
            dsn, ssl_option = _prepare_dsn(DATABASE_URL)
            _pool = await asyncpg.create_pool(
                dsn=dsn,
                ssl=ssl_option,
                # min_size=2 держит пару соединений всегда открытыми, чтобы
                # обычный запрос не платил цену TCP+TLS хендшейка с БД
                # (нередко 100-300 мс на управляемых Postgres-провайдерах),
                # который иначе случался бы при каждом "холодном" всплеске
                # активности после периода простоя.
                min_size=2,
                max_size=10,
                command_timeout=10,
            )
    return _pool


async def keepalive_loop(interval_seconds: int = 180) -> None:
    """Не даёт БД "заснуть" между сообщениями пользователей.

    Neon (как и большинство serverless Postgres) отключает вычислительный
    узел после нескольких минут без активных запросов, чтобы не тратить
    ресурсы впустую. Это и есть причина задержки ~5 секунд на /start или
    первое сообщение после долгого перерыва — Neon поднимает узел заново
    ("холодный старт"), и первый же запрос к БД ждёт этого пробуждения.
    Приложение здесь ничего "не тормозит" — простой SELECT 1 каждые
    interval_seconds не даёт простою наступить, и все запросы (включая
    самый первый после паузы) остаются быстрыми.

    Запускается как фоновая задача сразу после старта пула (см. bot.py:main
    и app.py:run_bot). interval_seconds=180 (3 минуты) — с запасом меньше
    типичного тайм-аута автоотключения (обычно 5 минут)."""
    pool = await get_pool()
    while True:
        try:
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
        except asyncio.CancelledError:
            raise
        except Exception:
            # Сеть/БД могли на секунду моргнуть — не роняем фоновую задачу,
            # следующая попытка через interval_seconds всё исправит.
            pass
        await asyncio.sleep(interval_seconds)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def _to_dict(record) -> Optional[dict]:
    return dict(record) if record is not None else None


def _to_dict_list(records) -> list:
    return [dict(r) for r in records]


# ---------------------------------------------------------------------------
# USERS
# ---------------------------------------------------------------------------

async def get_user_by_telegram_id(telegram_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
    return _to_dict(row)


async def create_user(
    telegram_id: int,
    name: str,
    phone: str,
    level: str,
    age: Optional[int] = None,
    city: Optional[str] = None,
    has_inventory: Optional[bool] = None,
    needs_rules: Optional[bool] = None,
) -> dict:
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO users
           (telegram_id, name, phone, level, age, city, has_inventory, needs_rules)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *""",
        telegram_id, name, phone, level, age, city, has_inventory, needs_rules,
    )
    return _to_dict(row)


async def update_user(
    telegram_id: int,
    name: str,
    phone: str,
    level: str,
    age: Optional[int] = None,
    city: Optional[str] = None,
    has_inventory: Optional[bool] = None,
    needs_rules: Optional[bool] = None,
) -> dict:
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE users
           SET name = $1, phone = $2, level = $3,
               age = $4, city = $5, has_inventory = $6, needs_rules = $7
           WHERE telegram_id = $8 RETURNING *""",
        name, phone, level, age, city, has_inventory, needs_rules, telegram_id,
    )
    return _to_dict(row)


# ---------------------------------------------------------------------------
# GAMES
# ---------------------------------------------------------------------------

async def get_upcoming_games() -> list:
    """Игры, которые ещё не прошли, отсортированные по дате (без счётчика мест)."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT * FROM games
        WHERE (game_date + game_time) >= NOW()
        ORDER BY game_date, game_time
        """
    )
    return _to_dict_list(rows)


async def get_upcoming_games_with_slots() -> list:
    """Один агрегирующий запрос вместо N+1: сразу считает занятые места
    для каждой ближайшей игры. Результат кладётся в кэш в bot.py — поэтому
    сортировка и фильтр по свободным местам применяются здесь, ДО
    кэширования: то, что попадает в кэш, уже готово к показу как есть.

    SUM(slots_count), а не COUNT(*): с фичей «Сколько мест? (1-4)» одна
    заявка может занимать сразу несколько мест, поэтому число заявок больше
    не равно числу занятых мест.

    ORDER BY g.game_date, g.game_time — сначала ближайшие игры.

    COALESCE(bk.taken, 0) < g.total_slots — игры, где все места уже заняты,
    полностью исключаются из результата (а не просто помечаются "мест нет"
    в тексте). Как только кто-то отменит бронь, освободившееся место сразу
    видно новым пользователям — кэш инвалидируется при любой брони/отмене
    (см. _invalidate_games_cache в bot.py), так что устаревших данных
    дольше TTL/факта изменения не бывает."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT g.*, COALESCE(bk.taken, 0) AS taken
        FROM games g
        LEFT JOIN (
            SELECT game_id, SUM(slots_count) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        WHERE (g.game_date + g.game_time) >= NOW()
          AND COALESCE(bk.taken, 0) < g.total_slots
        ORDER BY g.game_date, g.game_time
        """
    )
    return _to_dict_list(rows)


async def get_game_by_id(game_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM games WHERE id = $1", game_id)
    return _to_dict(row)


async def count_bookings_for_game(game_id: int) -> int:
    """Сколько мест уже занято на игре (SUM(slots_count), не число заявок)."""
    pool = await get_pool()
    taken = await pool.fetchval(
        "SELECT COALESCE(SUM(slots_count), 0) FROM bookings WHERE game_id = $1 AND status != 'отменена'",
        game_id,
    )
    return int(taken)


async def get_games_needing_reminder_24h() -> list:
    """Игры, для которых пора отправить напоминание за 24 часа (окно
    23-25ч, чтобы точно поймать нужный момент при ежечасной/более частой
    проверке планировщиком) и оно ещё не было отправлено."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT * FROM games
        WHERE reminder_sent = FALSE
          AND (game_date + game_time) BETWEEN NOW() + INTERVAL '23 hours'
                                            AND NOW() + INTERVAL '25 hours'
        """
    )
    return _to_dict_list(rows)


async def get_games_needing_reminder_2h() -> list:
    """Аналогично get_games_needing_reminder_24h, но для напоминания за 2
    часа. Окно уже (1ч45м-2ч15м), поэтому планировщик должен проверять чаще
    (см. main() в bot.py/app.py — интервал 15 минут)."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT * FROM games
        WHERE reminder_2h_sent = FALSE
          AND (game_date + game_time) BETWEEN NOW() + INTERVAL '1 hour 45 minutes'
                                            AND NOW() + INTERVAL '2 hours 15 minutes'
        """
    )
    return _to_dict_list(rows)


async def mark_reminder_24h_sent(game_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE games SET reminder_sent = TRUE WHERE id = $1", game_id)


async def mark_reminder_2h_sent(game_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE games SET reminder_2h_sent = TRUE WHERE id = $1", game_id)


# ---------------------------------------------------------------------------
# BOOKINGS
# ---------------------------------------------------------------------------

MAX_SLOTS_PER_BOOKING = 4


async def create_booking_safe(user_id: int, game_id: int, slots_count: int = 1) -> dict:
    """Атомарно проверяет наличие свободных мест и создаёт заявку в одной
    транзакции с блокировкой строки игры (SELECT ... FOR UPDATE), чтобы
    исключить race condition при одновременной записи нескольких игроков
    на последнее свободное место.

    slots_count — сколько мест игрок бронирует за один раз (фича «Сколько
    мест? (1-4)» в боте). Валидируем диапазон и здесь же, а не только в
    интерфейсе бота — это защита на уровне данных на случай ошибки/подмены
    callback_data.

    Возвращает dict {"status": "ok"|"full"|"duplicate"|"not_found",
    "booking": ..., "game": ...}.

    Отдаём game здесь же (мы и так уже сходили за ней в БД внутри
    транзакции) — раньше bot.py делал отдельный предварительный
    get_game_by_id() перед вызовом этой функции только чтобы получить те же
    данные для текста сообщения/уведомления админу, то есть на каждую
    заявку тратился лишний round-trip к БД.
    """
    slots_count = max(1, min(MAX_SLOTS_PER_BOOKING, int(slots_count)))

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            game = await conn.fetchrow(
                "SELECT * FROM games WHERE id = $1 FOR UPDATE", game_id
            )
            if game is None:
                return {"status": "not_found", "booking": None, "game": None}
            game_dict = _to_dict(game)

            existing = await conn.fetchrow(
                """SELECT * FROM bookings
                   WHERE user_id = $1 AND game_id = $2 AND status != 'отменена'""",
                user_id, game_id,
            )
            if existing is not None:
                return {"status": "duplicate", "booking": _to_dict(existing), "game": game_dict}

            taken = await conn.fetchval(
                "SELECT COALESCE(SUM(slots_count), 0) FROM bookings WHERE game_id = $1 AND status != 'отменена'",
                game_id,
            )
            if taken + slots_count > game["total_slots"]:
                return {"status": "full", "booking": None, "game": game_dict}

            booking = await conn.fetchrow(
                """INSERT INTO bookings (user_id, game_id, status, slots_count)
                   VALUES ($1, $2, 'новая', $3) RETURNING *""",
                user_id, game_id, slots_count,
            )
            return {"status": "ok", "booking": _to_dict(booking), "game": game_dict}


async def get_active_bookings_for_user(user_id: int) -> list:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT b.*, g.game_date, g.game_time, g.location, g.price
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        WHERE b.user_id = $1 AND b.status != 'отменена'
        ORDER BY g.game_date, g.game_time
        """,
        user_id,
    )
    return _to_dict_list(rows)


async def get_booking_by_id(booking_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM bookings WHERE id = $1", booking_id)
    return _to_dict(row)


async def cancel_booking_owned(booking_id: int, user_id: int) -> str:
    """Отменяет заявку только если она принадлежит указанному пользователю.
    Защита от IDOR: раньше отмена работала по одному booking_id без проверки
    владельца, что позволяло отменить чужую запись, зная/подобрав id.

    Возвращает "ok", "not_found" или "forbidden".
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            booking = await conn.fetchrow(
                "SELECT * FROM bookings WHERE id = $1 FOR UPDATE", booking_id
            )
            if booking is None:
                return "not_found"
            if booking["user_id"] != user_id:
                return "forbidden"
            await conn.execute(
                "UPDATE bookings SET status = 'отменена' WHERE id = $1", booking_id
            )
            return "ok"


async def get_participants_for_game(game_id: int) -> list:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT u.telegram_id, u.name, b.id AS booking_id
        FROM bookings b
        JOIN users u ON u.id = b.user_id
        WHERE b.game_id = $1 AND b.status != 'отменена'
        """,
        game_id,
    )
    return _to_dict_list(rows)


# ---------------------------------------------------------------------------
# PAYMENTS — оплата через бота (см. payment_provider.py и bot.py)
# ---------------------------------------------------------------------------

async def create_payment(booking_id: int, amount: float, method: Optional[str] = None) -> dict:
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO payments (booking_id, amount, status, method)
           VALUES ($1, $2, 'ожидает', $3) RETURNING *""",
        booking_id, amount, method,
    )
    return _to_dict(row)


async def get_payment_by_id(payment_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM payments WHERE id = $1", payment_id)
    return _to_dict(row)


async def set_payment_method(payment_id: int, method: str) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE payments SET method = $1 WHERE id = $2", method, payment_id)


async def confirm_payment(payment_id: int) -> None:
    """Используется автоматическим подтверждением оплаты картой через
    реальный Telegram Payments provider (см. process_successful_payment в
    bot.py) — если провайдер не подключён, подтверждение делает
    администратор вручную в CRM (db.confirm_payment в database.py)."""
    pool = await get_pool()
    await pool.execute("UPDATE payments SET status = 'подтверждена' WHERE id = $1", payment_id)


# ---------------------------------------------------------------------------
# STATISTICS
# ---------------------------------------------------------------------------

async def get_user_statistics(user_id: int) -> dict:
    """Статистика игрока — раньше требовала 4 отдельных запроса,
    теперь один запрос с условными агрегатами (FILTER)."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'отменена') AS cancelled,
            COUNT(*) FILTER (WHERE status = 'посещена') AS attended,
            (
                SELECT COUNT(DISTINCT b.id)
                FROM bookings b
                JOIN payments p ON p.booking_id = b.id
                WHERE b.user_id = $1 AND p.status = 'подтверждена'
            ) AS paid
        FROM bookings
        WHERE user_id = $1
        """,
        user_id,
    )

    total = row["total"]
    cancelled = row["cancelled"]
    attended = row["attended"]
    paid = row["paid"]

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
