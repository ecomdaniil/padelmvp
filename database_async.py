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
import logging
import os
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Клуб работает по московскому времени (см. database.py). Сессия БД — UTC;
# для сравнений с game_date+game_time всегда берём московский «сейчас».
APP_TIMEZONE = "Europe/Moscow"
_LOCAL_NOW_EXPR = f"(NOW() AT TIME ZONE '{APP_TIMEZONE}')"

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()


def _prepare_dsn(url: str):
    """asyncpg не всегда понимает sslmode=/channel_binding= в DSN одинаково,
    поэтому вынимаем их явно: ssl передаём параметром ssl=, channel_binding
    отбрасываем (нужен libpq, asyncpg его не использует)."""
    parts = urlsplit(url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    ssl_mode = None
    remaining = []
    for key, value in pairs:
        if key == "sslmode":
            ssl_mode = value
        elif key in {"channel_binding"}:
            continue
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
                # На Render free держим min_size=1 — иначе CRM+бот+два пула
                # легко ловят SIGKILL OOM (~512MB).
                min_size=int(os.getenv("ASYNC_DB_POOL_MIN_SIZE", "2")),
                max_size=int(os.getenv("ASYNC_DB_POOL_MAX_SIZE", "10")),
                command_timeout=10,
                timeout=float(os.getenv("ASYNC_DB_CONNECT_TIMEOUT", "10")),
            )
    return _pool


async def keepalive_loop(interval_seconds: int = 45) -> None:
    """Не даёт БД "заснуть" между сообщениями пользователей.

    Neon (как и большинство serverless Postgres) отключает вычислительный
    узел после нескольких минут без активных запросов — отсюда задержка
    ~5–8 с на /start или первое сообщение после паузы. SELECT 1 сразу при
    старте и далее каждые interval_seconds (45с по умолчанию) не даёт
    простою наступить.

    Запускается как фоновая задача сразу после старта пула (см. bot.py:main
    и app.py:run_bot)."""
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
               age = $4, city = COALESCE($5, city),
               has_inventory = $6, needs_rules = $7
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
        f"""
        SELECT * FROM games
        WHERE (game_date + game_time) >= {_LOCAL_NOW_EXPR}
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

    (COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0)) < g.total_slots
    — игры, где все места уже заняты, полностью исключаются из результата
    (а не просто помечаются "мест нет" в тексте). Занятость считается как
    СУММА реальных бронирований и ручного booked_places, которое админ
    может выставить в CRM для мест, занятых мимо бота (например, по
    телефону/на месте) — это ДОПОЛНИТЕЛЬНЫЕ места сверх бронирований, а не
    альтернативная/перекрывающая их величина. Боту должно быть безразлично,
    кто занял места (бот или ручная правка) — при 1 месте через бота и 1
    вручную на игру с total_slots=2 игра должна считаться полностью занятой.
    LEAST(..., g.total_slots) — на случай, если сумма превысит total_slots
    (например, из-за гонки бронирования и ручной правки), не показываем
    отрицательное количество свободных мест. Как только кто-то отменит бронь
    (или админ уменьшит booked_places), освободившееся место сразу видно
    новым пользователям — кэш инвалидируется при любой брони/отмене (см.
    _invalidate_games_cache в bot.py) и при сохранении игры в CRM, так что
    устаревших данных дольше TTL/факта изменения не бывает."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT g.*,
               co.name AS coach_name,
               co.emoji AS coach_emoji,
               LEAST(
                   COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0), g.total_slots
               ) AS taken
        FROM games g
        LEFT JOIN coaches co ON co.id = g.coach_id
        LEFT JOIN (
            SELECT game_id, SUM(slots_count) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        WHERE (g.game_date + g.game_time) >= {_LOCAL_NOW_EXPR}
          AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
          AND (COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0)) < g.total_slots
        ORDER BY g.game_date, g.game_time
        """
    )
    return _to_dict_list(rows)


async def get_active_coaches() -> list:
    """Активные тренеры для бота (раздел «Тренеры»)."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM coaches
           WHERE is_active = TRUE
           ORDER BY sort_order, name"""
    )
    return _to_dict_list(rows)


async def get_coach_by_id(coach_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM coaches WHERE id = $1", coach_id)
    return _to_dict(row)


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
    """Напоминание ~за сутки: идеальное окно 23–25ч, нижняя граница
    расширена до 2ч15м (catch-up), пока не сработает 2ч-напоминание."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT * FROM games
        WHERE reminder_sent = FALSE
          AND COALESCE(underfill_cancelled, FALSE) = FALSE
          AND (game_date + game_time) BETWEEN {_LOCAL_NOW_EXPR} + INTERVAL '2 hours 15 minutes'
                                            AND {_LOCAL_NOW_EXPR} + INTERVAL '25 hours'
        """
    )
    return _to_dict_list(rows)


async def get_games_needing_reminder_2h() -> list:
    """Напоминание ~за 2 часа: верх 2ч15м, низ 0 (catch-up до старта)."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT * FROM games
        WHERE reminder_2h_sent = FALSE
          AND COALESCE(underfill_cancelled, FALSE) = FALSE
          AND (game_date + game_time) BETWEEN {_LOCAL_NOW_EXPR}
                                            AND {_LOCAL_NOW_EXPR} + INTERVAL '2 hours 15 minutes'
        """
    )
    return _to_dict_list(rows)


_TAKEN_EXPR = (
    "(COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0))"
)


async def _games_underfill_in_window(start_interval: str, end_interval: str, extra_where: str) -> list:
    """Обычные игры с недобором состава в заданном окне до старта (Москва).
    Тренировки сюда не попадают — их не отменяем из‑за недобора."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT g.*,
               LEAST({_TAKEN_EXPR}, g.total_slots) AS taken
        FROM games g
        LEFT JOIN (
            SELECT game_id, SUM(slots_count) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        WHERE COALESCE(g.underfill_cancelled, FALSE) = FALSE
          AND COALESCE(g.event_type, 'game') = 'game'
          AND {_TAKEN_EXPR} < g.total_slots
          AND {_TAKEN_EXPR} > 0
          AND (g.game_date + g.game_time) BETWEEN {_LOCAL_NOW_EXPR} + INTERVAL '{start_interval}'
                                            AND {_LOCAL_NOW_EXPR} + INTERVAL '{end_interval}'
          AND {extra_where}
        ORDER BY g.game_date, g.game_time
        """
    )
    return _to_dict_list(rows)


async def get_games_needing_underfill_warn_3h() -> list:
    """За ~3 часа: недобор. Catch-up до окна автоотмены (~1ч15м)."""
    return await _games_underfill_in_window(
        "1 hour 15 minutes",
        "3 hours 15 minutes",
        "COALESCE(g.underfill_warn_3h_sent, FALSE) = FALSE",
    )


async def get_games_needing_underfill_cancel_1h() -> list:
    """Автоотмена недобора за ~час до старта.
    Нижняя граница −90 мин — catch-up, если job проснулся уже после старта."""
    return await _games_underfill_in_window(
        "-90 minutes",
        "1 hour 15 minutes",
        "TRUE",
    )


async def mark_underfill_warn_3h_sent(game_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE games SET underfill_warn_3h_sent = TRUE WHERE id = $1", game_id
    )


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

    За менее чем 1 час до старта нельзя взять часть мест: только выкуп
    всех оставшихся (must_fill_all), иначе игра уйдёт в недобор.

    Возвращает dict {"status": "ok"|"full"|"duplicate"|"not_found"|"must_fill_all",
    "booking": ..., "game": ..., "free_slots": int?}.

    Отдаём game здесь же (мы и так уже сходили за ней в БД внутри
    транзакции) — раньше bot.py делал отдельный предварительный
    get_game_by_id() перед вызовом этой функции только чтобы получить те же
    данные для текста сообщения/уведомления админу, то есть на каждую
    заявку тратился лишний round-trip к БД.
    """
    requested_slots = max(1, int(slots_count))

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            game = await conn.fetchrow(
                "SELECT * FROM games WHERE id = $1 FOR UPDATE", game_id
            )
            if game is None:
                return {"status": "not_found", "booking": None, "game": None}
            game_dict = _to_dict(game)
            if game_dict.get("underfill_cancelled"):
                return {"status": "not_found", "booking": None, "game": game_dict}

            # Нельзя записаться на уже начавшуюся/прошедшую игру (по Москве).
            timing = await conn.fetchrow(
                f"""
                SELECT
                    (game_date + game_time) >= {_LOCAL_NOW_EXPR} AS still_upcoming,
                    (game_date + game_time) < {_LOCAL_NOW_EXPR} + INTERVAL '1 hour'
                        AS within_last_hour
                FROM games WHERE id = $1
                """,
                game_id,
            )
            if not timing or not timing["still_upcoming"]:
                return {"status": "not_found", "booking": None, "game": game_dict}

            existing = await conn.fetchrow(
                """SELECT * FROM bookings
                   WHERE user_id = $1 AND game_id = $2 AND status != 'отменена'""",
                user_id, game_id,
            )
            if existing is not None:
                # Повторный клик / гонка: не ошибка — отдаём текущую бронь,
                # чтобы бот мог продолжить оплату, а не «ты уже записан».
                latest_payment = await conn.fetchrow(
                    """SELECT * FROM payments
                       WHERE booking_id = $1
                       ORDER BY id DESC LIMIT 1""",
                    existing["id"],
                )
                return {
                    "status": "duplicate",
                    "booking": _to_dict(existing),
                    "game": game_dict,
                    "payment": _to_dict(latest_payment) if latest_payment else None,
                }

            taken = await conn.fetchval(
                "SELECT COALESCE(SUM(slots_count), 0) FROM bookings WHERE game_id = $1 AND status != 'отменена'",
                game_id,
            )
            # Учитываем и ручной booked_places (админ мог занять места в CRM
            # мимо бота, например по телефону) — это ДОПОЛНИТЕЛЬНЫЕ места
            # сверх реальных бронирований, поэтому складываем, а не берём
            # максимум, иначе бот пустил бы запись сверх реального лимита.
            effective_taken = int(taken) + int(game_dict.get("booked_places") or 0)
            free_slots = int(game["total_slots"]) - effective_taken
            if free_slots <= 0:
                return {"status": "full", "booking": None, "game": game_dict, "free_slots": 0}

            # Меньше часа до старта — только полный выкуп оставшихся мест
            # (даже если их > MAX_SLOTS_PER_BOOKING). Для тренировок правило
            # недобора не действует — частичная запись разрешена до старта.
            is_training = (game_dict.get("event_type") or "game") == "training"
            if timing["within_last_hour"] and not is_training:
                if requested_slots != free_slots:
                    return {
                        "status": "must_fill_all",
                        "booking": None,
                        "game": game_dict,
                        "free_slots": free_slots,
                        "total_slots": int(game["total_slots"]),
                    }
                slots_count = free_slots
            else:
                slots_count = min(MAX_SLOTS_PER_BOOKING, requested_slots)
                if slots_count > free_slots:
                    return {
                        "status": "full",
                        "booking": None,
                        "game": game_dict,
                        "free_slots": free_slots,
                    }

            booking = await conn.fetchrow(
                """INSERT INTO bookings (user_id, game_id, status, slots_count)
                   VALUES ($1, $2, 'новая', $3) RETURNING *""",
                user_id, game_id, slots_count,
            )
            # Платёж в той же транзакции — иначе при сбое после INSERT брони
            # пользователь «записан», но не видит оплату и ловит duplicate.
            amount = float(game_dict["price"]) * int(slots_count)
            payment = await conn.fetchrow(
                """INSERT INTO payments (booking_id, amount, status, method)
                   VALUES ($1, $2, 'ожидает', NULL) RETURNING *""",
                booking["id"], amount,
            )
            new_taken = effective_taken + slots_count
            return {
                "status": "ok",
                "booking": _to_dict(booking),
                "game": game_dict,
                "payment": _to_dict(payment),
                "taken": new_taken,
                "total_slots": int(game["total_slots"]),
            }


async def get_game_slot_offer(game_id: int) -> Optional[dict]:
    """Данные для экрана «Сколько мест?»: свободно мест и флаг «меньше часа»."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT g.*,
               LEAST(
                   COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0),
                   g.total_slots
               ) AS taken,
               (g.game_date + g.game_time) >= {_LOCAL_NOW_EXPR} AS still_upcoming,
               (g.game_date + g.game_time) < {_LOCAL_NOW_EXPR} + INTERVAL '1 hour'
                   AS within_last_hour
        FROM games g
        LEFT JOIN (
            SELECT game_id, SUM(slots_count) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        WHERE g.id = $1
        """,
        game_id,
    )
    if row is None:
        return None
    data = _to_dict(row)
    if data.get("underfill_cancelled") or not data.get("still_upcoming"):
        return None
    taken = int(data.get("taken") or 0)
    total = int(data["total_slots"])
    free_slots = max(0, total - taken)
    if free_slots <= 0:
        return None
    # Тренировки не отменяем по недобору — в последний час частичная запись ок.
    is_training = (data.get("event_type") or "game") == "training"
    return {
        "game": data,
        "taken": taken,
        "total_slots": total,
        "free_slots": free_slots,
        "within_last_hour": bool(data.get("within_last_hour")) and not is_training,
    }


async def get_active_bookings_for_user(user_id: int) -> list:
    """Только предстоящие (ещё не начавшиеся) незакрытые записи.
    free_slots — сколько мест ещё свободно на этой игре (для «Докупить места»)."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT b.*, g.game_date, g.game_time, g.location, g.price, g.total_slots,
               g.event_type, g.title,
               GREATEST(
                   0,
                   g.total_slots
                   - COALESCE(bk.taken, 0)
                   - COALESCE(g.booked_places, 0)
               ) AS free_slots
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        LEFT JOIN (
            SELECT game_id, SUM(slots_count) AS taken
            FROM bookings
            WHERE status != 'отменена'
            GROUP BY game_id
        ) bk ON bk.game_id = g.id
        WHERE b.user_id = $1
          AND b.status != 'отменена'
          AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
          AND (g.game_date + g.game_time) >= {_LOCAL_NOW_EXPR}
        ORDER BY g.game_date, g.game_time
        """,
        user_id,
    )
    return _to_dict_list(rows)


async def increase_booking_slots_safe(
    user_id: int, booking_id: int, extra_slots: int,
) -> dict:
    """Докупить места к существующей брони на ту же игру.

    Атомарно (FOR UPDATE на игре): проверяет свободные места, увеличивает
    slots_count, создаёт/увеличивает платёж «ожидает» на сумму доплаты.

    Возвращает {"status": "ok"|"full"|"not_found"|"forbidden"|"must_fill_all",
                "booking", "game", "payment", "extra_slots", "extra_amount", ...}.
    """
    requested = max(1, int(extra_slots))
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            booking = await conn.fetchrow(
                """SELECT * FROM bookings
                   WHERE id = $1 AND user_id = $2 AND status != 'отменена'
                   FOR UPDATE""",
                booking_id, user_id,
            )
            if booking is None:
                return {"status": "not_found", "booking": None, "game": None}

            game = await conn.fetchrow(
                "SELECT * FROM games WHERE id = $1 FOR UPDATE",
                booking["game_id"],
            )
            if game is None:
                return {"status": "not_found", "booking": None, "game": None}
            game_dict = _to_dict(game)
            if game_dict.get("underfill_cancelled"):
                return {"status": "not_found", "booking": _to_dict(booking), "game": game_dict}

            timing = await conn.fetchrow(
                f"""
                SELECT
                    (game_date + game_time) >= {_LOCAL_NOW_EXPR} AS still_upcoming,
                    (game_date + game_time) < {_LOCAL_NOW_EXPR} + INTERVAL '1 hour'
                        AS within_last_hour
                FROM games WHERE id = $1
                """,
                game["id"],
            )
            if not timing or not timing["still_upcoming"]:
                return {
                    "status": "not_found",
                    "booking": _to_dict(booking),
                    "game": game_dict,
                }

            taken = await conn.fetchval(
                """SELECT COALESCE(SUM(slots_count), 0) FROM bookings
                   WHERE game_id = $1 AND status != 'отменена'""",
                game["id"],
            )
            effective_taken = int(taken) + int(game_dict.get("booked_places") or 0)
            free_slots = int(game["total_slots"]) - effective_taken
            if free_slots <= 0:
                return {
                    "status": "full",
                    "booking": _to_dict(booking),
                    "game": game_dict,
                    "free_slots": 0,
                }

            if timing["within_last_hour"] and (game_dict.get("event_type") or "game") != "training":
                if requested != free_slots:
                    return {
                        "status": "must_fill_all",
                        "booking": _to_dict(booking),
                        "game": game_dict,
                        "free_slots": free_slots,
                        "total_slots": int(game["total_slots"]),
                    }
                slots_to_add = free_slots
            else:
                slots_to_add = min(MAX_SLOTS_PER_BOOKING, requested)
                if slots_to_add > free_slots:
                    return {
                        "status": "full",
                        "booking": _to_dict(booking),
                        "game": game_dict,
                        "free_slots": free_slots,
                    }

            extra_amount = float(game_dict["price"]) * int(slots_to_add)
            updated_booking = await conn.fetchrow(
                """UPDATE bookings
                   SET slots_count = slots_count + $1
                   WHERE id = $2
                   RETURNING *""",
                slots_to_add, booking_id,
            )

            # Только «открытый» неоплаченный счёт можно увеличить.
            # Уже оплаченный (player_notified_at) или подтверждённый админом
            # НЕ трогаем — для доплаты новый платёж. Старые provider_id
            # (ЮKassa) сбрасываем: эквайринг отключён, уникальный индекс
            # на один open-pending иначе ломал докупку.
            open_pending = await conn.fetchrow(
                """SELECT * FROM payments
                   WHERE booking_id = $1
                     AND status = 'ожидает'
                     AND player_notified_at IS NULL
                   ORDER BY id DESC LIMIT 1
                   FOR UPDATE""",
                booking_id,
            )
            if open_pending is not None:
                payment = await conn.fetchrow(
                    """UPDATE payments
                       SET amount = amount + $1,
                           provider_payment_id = NULL,
                           confirmation_url = NULL
                       WHERE id = $2
                         AND status = 'ожидает'
                         AND player_notified_at IS NULL
                       RETURNING *""",
                    extra_amount, open_pending["id"],
                )
            else:
                payment = await conn.fetchrow(
                    """INSERT INTO payments (booking_id, amount, status, method)
                       VALUES ($1, $2, 'ожидает', NULL) RETURNING *""",
                    booking_id, extra_amount,
                )

            new_taken = effective_taken + slots_to_add
            return {
                "status": "ok",
                "booking": _to_dict(updated_booking),
                "game": game_dict,
                "payment": _to_dict(payment) if payment else None,
                "extra_slots": slots_to_add,
                "extra_amount": extra_amount,
                "taken": new_taken,
                "total_slots": int(game["total_slots"]),
                "free_slots": free_slots - slots_to_add,
            }


async def get_past_bookings_for_user(user_id: int) -> list:
    """Сыгранные / уже прошедшие игры (не отменённые записи)."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT b.*, g.game_date, g.game_time, g.location, g.price, g.total_slots,
               g.event_type, g.title
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        WHERE b.user_id = $1
          AND b.status != 'отменена'
          AND (g.game_date + g.game_time) < {_LOCAL_NOW_EXPR}
        ORDER BY g.game_date DESC, g.game_time DESC
        """,
        user_id,
    )
    return _to_dict_list(rows)


async def get_club_admin_telegram_id() -> Optional[int]:
    """Telegram ID админа из CRM (club_info), если задан."""
    pool = await get_pool()
    value = await pool.fetchval(
        """
        SELECT admin_telegram_id FROM club_info
        ORDER BY id DESC LIMIT 1
        """
    )
    return int(value) if value is not None else None


async def set_club_admin_telegram_id(telegram_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE club_info
        SET admin_telegram_id = $1, updated_at = NOW()
        WHERE id = (SELECT id FROM club_info ORDER BY id DESC LIMIT 1)
        """,
        int(telegram_id),
    )


async def cancel_underfilled_game(game_id: int) -> dict:
    """Автоотмена игры из‑за недобора: все активные брони → «отменена»,
    подтверждённые оплаты → «возврат». Идемпотентно (повторный вызов — already).

    Возвращает:
      status: not_found | already | full | ok
      game, cancelled: [{telegram_id, name, booking_id, refunded, amount}]
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            game = await conn.fetchrow(
                "SELECT * FROM games WHERE id = $1 FOR UPDATE", game_id
            )
            if game is None:
                return {"status": "not_found", "game": None, "cancelled": []}
            if (game["event_type"] or "game") == "training":
                # Тренировки не автоотменяем при недоборе.
                return {"status": "full", "game": _to_dict(game), "cancelled": []}
            if game["underfill_cancelled"]:
                return {"status": "already", "game": _to_dict(game), "cancelled": []}

            taken = await conn.fetchval(
                """SELECT COALESCE(SUM(slots_count), 0) FROM bookings
                   WHERE game_id = $1 AND status != 'отменена'""",
                game_id,
            )
            effective = int(taken) + int(game["booked_places"] or 0)
            if effective >= game["total_slots"]:
                return {"status": "full", "game": _to_dict(game), "cancelled": []}

            bookings = await conn.fetch(
                """
                SELECT b.id AS booking_id, u.telegram_id, u.name
                FROM bookings b
                JOIN users u ON u.id = b.user_id
                WHERE b.game_id = $1 AND b.status != 'отменена'
                FOR UPDATE OF b
                """,
                game_id,
            )

            cancelled = []
            for row in bookings:
                payments = await conn.fetch(
                    """SELECT id, amount, status, player_notified_at FROM payments
                       WHERE booking_id = $1 FOR UPDATE""",
                    row["booking_id"],
                )
                await conn.execute(
                    "UPDATE bookings SET status = 'отменена' WHERE id = $1",
                    row["booking_id"],
                )
                refunded = False
                amount_sum = 0.0
                for payment in payments:
                    status = payment["status"]
                    notified = payment["player_notified_at"] is not None
                    if status == "подтверждена" or (status == "ожидает" and notified):
                        await conn.execute(
                            """UPDATE payments
                                  SET status = 'возврат',
                                      admin_attention_at = NOW()
                                WHERE id = $1""",
                            payment["id"],
                        )
                        refunded = True
                        if payment["amount"] is not None:
                            amount_sum += float(payment["amount"])
                    elif status == "ожидает":
                        await conn.execute(
                            "DELETE FROM payments WHERE id = $1 AND status = 'ожидает'",
                            payment["id"],
                        )
                cancelled.append({
                    "telegram_id": row["telegram_id"],
                    "name": row["name"],
                    "booking_id": row["booking_id"],
                    "refunded": refunded,
                    "amount": amount_sum if refunded else None,
                })

            await conn.execute(
                """UPDATE games
                   SET underfill_cancelled = TRUE, underfill_warn_3h_sent = TRUE
                   WHERE id = $1""",
                game_id,
            )
            game_dict = _to_dict(game)
            game_dict["underfill_cancelled"] = True
            if any(item.get("refunded") for item in cancelled):
                try:
                    import database as db_sync
                    db_sync.clear_badge_cache()
                except Exception:
                    pass
            return {"status": "ok", "game": game_dict, "cancelled": cancelled, "taken": effective}


async def get_booking_by_id(booking_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM bookings WHERE id = $1", booking_id)
    return _to_dict(row)


async def get_booking_cancel_info(booking_id: int, user_id: int) -> dict:
    """Данные для экрана подтверждения отмены в боте — без самой отмены.

    Возвращает:
        {"status": "not_found"|"forbidden"|"ok", ...}
        при ok:
          refund_window — до игры больше 12 часов (локальное APP_TIMEZONE)
          payment_pending_confirm — игрок уже нажал «Я оплатил», но админ
            ещё не подтвердил (status='ожидает' и player_notified_at заполнен):
            в этом окне отмена запрещена
          had_confirmed_payment — есть платёж со статусом «подтверждена»
          game — dict игры
    """
    pool = await get_pool()
    booking = await pool.fetchrow("SELECT * FROM bookings WHERE id = $1", booking_id)
    if booking is None:
        return {"status": "not_found"}
    if booking["user_id"] != user_id:
        return {"status": "forbidden"}
    if booking["status"] == "отменена":
        return {"status": "not_found"}

    game = await pool.fetchrow(
        f"""
        SELECT *,
               ((game_date + game_time) - {_LOCAL_NOW_EXPR}) > INTERVAL '12 hours' AS refund_window,
               (game_date + game_time) >= {_LOCAL_NOW_EXPR} AS still_upcoming
        FROM games
        WHERE id = $1
        """,
        booking["game_id"],
    )
    if game is None:
        return {"status": "not_found"}
    if not game["still_upcoming"] or game.get("underfill_cancelled"):
        return {"status": "too_late", "game": _to_dict(game)}
    payment_rows = await pool.fetch(
        """
        SELECT * FROM payments
        WHERE booking_id = $1
        ORDER BY id DESC
        """,
        booking_id,
    )
    payments = [_to_dict(p) for p in payment_rows]
    pending_confirm = any(
        p.get("status") == "ожидает" and p.get("player_notified_at") is not None
        for p in payments
    )
    had_confirmed = any(p.get("status") == "подтверждена" for p in payments)
    latest = payments[0] if payments else None
    return {
        "status": "ok",
        "refund_window": bool(game["refund_window"]),
        "payment_pending_confirm": pending_confirm,
        "had_confirmed_payment": had_confirmed,
        "game": _to_dict(game),
        "payment": latest,
        "booking": _to_dict(booking),
    }


async def cancel_payment_or_booking_owned(
    booking_id: int, user_id: int, payment_id: int,
) -> dict:
    """Отмена с экрана оплаты.

    Если отменяемый платёж — открытая доплата (ожидает, без player_notified_at),
    а по брони уже есть защищённые оплаты (подтверждена или ожидает с
    player_notified_at), то удаляем только доплату и откатываем лишние места.
    Иначе — полная отмена брони (cancel_booking_owned).

    Возвращает status:
      extra_cancelled — доплата снята, бронь и оплаченные места сохранены
      (+ поля booking, slots_kept, extra_slots_removed, ...)
      остальные — как у cancel_booking_owned
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            booking = await conn.fetchrow(
                "SELECT * FROM bookings WHERE id = $1 FOR UPDATE", booking_id
            )
            if booking is None:
                return {"status": "not_found"}
            if booking["user_id"] != user_id:
                return {"status": "forbidden"}
            if booking["status"] == "отменена":
                return {"status": "not_found"}

            payment = await conn.fetchrow(
                """SELECT * FROM payments
                   WHERE id = $1 AND booking_id = $2 FOR UPDATE""",
                payment_id, booking_id,
            )
            if payment is None:
                return {"status": "not_found"}

            game = await conn.fetchrow(
                f"""
                SELECT *,
                       (game_date + game_time) >= {_LOCAL_NOW_EXPR} AS still_upcoming
                FROM games WHERE id = $1
                """,
                booking["game_id"],
            )
            if game is None:
                return {"status": "not_found"}
            if not game["still_upcoming"] or game.get("underfill_cancelled"):
                return {"status": "too_late", "game": _to_dict(game)}

            is_open_unpaid = (
                payment["status"] == "ожидает"
                and payment["player_notified_at"] is None
            )
            if is_open_unpaid:
                protected = await conn.fetch(
                    """SELECT * FROM payments
                       WHERE booking_id = $1
                         AND id != $2
                         AND (
                             status = 'подтверждена'
                             OR (status = 'ожидает' AND player_notified_at IS NOT NULL)
                         )
                       FOR UPDATE""",
                    booking_id, payment_id,
                )
                if protected:
                    price = float(game["price"] or 0)
                    pending_amount = float(payment["amount"] or 0)
                    if price > 0.009:
                        slots_to_remove = int(round(pending_amount / price))
                        protected_amount = sum(float(p["amount"] or 0) for p in protected)
                        protected_slots = max(1, int(round(protected_amount / price)))
                    else:
                        slots_to_remove = 0
                        protected_slots = max(1, int(booking["slots_count"] or 1))

                    current_slots = int(booking["slots_count"] or 1)
                    new_slots = max(protected_slots, current_slots - max(0, slots_to_remove))
                    if new_slots < 1:
                        new_slots = protected_slots

                    extra_notify_id = booking.get("admin_extra_notify_message_id")
                    await conn.execute(
                        "DELETE FROM payments WHERE id = $1 AND status = 'ожидает'",
                        payment_id,
                    )
                    updated = await conn.fetchrow(
                        """UPDATE bookings
                           SET slots_count = $1,
                               admin_extra_notify_message_id = NULL
                           WHERE id = $2 RETURNING *""",
                        new_slots, booking_id,
                    )
                    return {
                        "status": "extra_cancelled",
                        "booking": _to_dict(updated),
                        "game": _to_dict(game),
                        "slots_kept": new_slots,
                        "extra_slots_removed": max(0, current_slots - new_slots),
                        "payment_deleted": True,
                        "had_payment": True,
                        "refund_eligible": False,
                        "refund_window": False,
                        "admin_notify_message_id": booking["admin_notify_message_id"],
                        "admin_extra_notify_message_id": extra_notify_id,
                        "payment": None,
                    }

    # Полная отмена брони (нет защищённых оплат / отмена всей записи).
    return await cancel_booking_owned(booking_id, user_id)


async def list_expired_unpaid_payment_ids(older_than_minutes: int = 5) -> list:
    """Открытые неоплаченные счета старше N минут (для автоотмены)."""
    minutes = max(1, int(older_than_minutes))
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT p.id
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        WHERE p.status = 'ожидает'
          AND p.player_notified_at IS NULL
          AND b.status != 'отменена'
          AND p.created_at <= NOW() - ($1 * INTERVAL '1 minute')
        ORDER BY p.created_at
        LIMIT 100
        """,
        minutes,
    )
    return [int(r["id"]) for r in rows]


async def expire_unpaid_payment(
    payment_id: int, *, older_than_minutes: int = 5,
) -> Optional[dict]:
    """Автоотмена неоплаченного счёта по таймауту.

    Если по брони есть защищённые оплаты — снимаем только доплату и лишние
    места. Иначе отменяем всю заявку. Не трогает платежи с player_notified_at
    (игрок уже оплатил / сообщил об оплате).

    Возвращает dict для уведомления игрока или None, если отменять нечего.
    """
    minutes = max(1, int(older_than_minutes))
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            payment = await conn.fetchrow(
                """SELECT * FROM payments WHERE id = $1 FOR UPDATE""",
                payment_id,
            )
            if payment is None:
                return None
            if payment["status"] != "ожидает" or payment["player_notified_at"] is not None:
                return None
            aged = await conn.fetchval(
                """SELECT created_at <= NOW() - ($1 * INTERVAL '1 minute')
                   FROM payments WHERE id = $2""",
                minutes, payment_id,
            )
            if not aged:
                return None

            booking = await conn.fetchrow(
                "SELECT * FROM bookings WHERE id = $1 FOR UPDATE",
                payment["booking_id"],
            )
            if booking is None or booking["status"] == "отменена":
                return None

            game = await conn.fetchrow(
                "SELECT * FROM games WHERE id = $1",
                booking["game_id"],
            )
            if game is None:
                return None

            user = await conn.fetchrow(
                "SELECT id, telegram_id, name FROM users WHERE id = $1",
                booking["user_id"],
            )
            if user is None or not user["telegram_id"]:
                return None

            provider_payment_id = payment["provider_payment_id"]
            admin_notify_message_id = booking["admin_notify_message_id"]
            admin_extra_notify_message_id = booking["admin_extra_notify_message_id"]

            protected = await conn.fetch(
                """SELECT * FROM payments
                   WHERE booking_id = $1
                     AND id != $2
                     AND (
                         status = 'подтверждена'
                         OR (status = 'ожидает' AND player_notified_at IS NOT NULL)
                     )
                   FOR UPDATE""",
                booking["id"], payment_id,
            )

            if protected:
                price = float(game["price"] or 0)
                pending_amount = float(payment["amount"] or 0)
                if price > 0.009:
                    slots_to_remove = int(round(pending_amount / price))
                    protected_amount = sum(float(p["amount"] or 0) for p in protected)
                    protected_slots = max(1, int(round(protected_amount / price)))
                else:
                    slots_to_remove = 0
                    protected_slots = max(1, int(booking["slots_count"] or 1))
                current_slots = int(booking["slots_count"] or 1)
                new_slots = max(protected_slots, current_slots - max(0, slots_to_remove))
                if new_slots < 1:
                    new_slots = protected_slots

                await conn.execute(
                    "DELETE FROM payments WHERE id = $1 AND status = 'ожидает'",
                    payment_id,
                )
                await conn.execute(
                    """UPDATE bookings
                       SET slots_count = $1,
                           admin_extra_notify_message_id = NULL
                     WHERE id = $2""",
                    new_slots, booking["id"],
                )
                mode = "extra"
            else:
                await conn.execute(
                    """UPDATE bookings
                       SET status = 'отменена',
                           admin_notify_message_id = NULL,
                           admin_extra_notify_message_id = NULL
                     WHERE id = $1""",
                    booking["id"],
                )
                await conn.execute(
                    """DELETE FROM payments
                       WHERE booking_id = $1
                         AND status = 'ожидает'
                         AND player_notified_at IS NULL""",
                    booking["id"],
                )
                mode = "full"

            return {
                "mode": mode,
                "payment_id": int(payment_id),
                "booking_id": int(booking["id"]),
                "telegram_id": int(user["telegram_id"]),
                "user_name": user["name"],
                "provider_payment_id": provider_payment_id,
                "admin_notify_message_id": admin_notify_message_id if mode == "full" else None,
                "admin_extra_notify_message_id": admin_extra_notify_message_id,
                "game": _to_dict(game),
            }


async def booking_has_protected_payment(
    booking_id: int, exclude_payment_id: Optional[int] = None,
) -> bool:
    """Есть ли по брони оплата, которую нельзя терять при отмене доплаты:
    подтверждённая или уже отмеченная игроком (PayMaster / «Я оплатил»)."""
    pool = await get_pool()
    if exclude_payment_id is None:
        row = await pool.fetchrow(
            """SELECT 1 FROM payments
               WHERE booking_id = $1
                 AND (
                     status = 'подтверждена'
                     OR (status = 'ожидает' AND player_notified_at IS NOT NULL)
                 )
               LIMIT 1""",
            booking_id,
        )
    else:
        row = await pool.fetchrow(
            """SELECT 1 FROM payments
               WHERE booking_id = $1
                 AND id != $2
                 AND (
                     status = 'подтверждена'
                     OR (status = 'ожидает' AND player_notified_at IS NOT NULL)
                 )
               LIMIT 1""",
            booking_id, exclude_payment_id,
        )
    return row is not None


async def cancel_booking_owned(booking_id: int, user_id: int) -> dict:
    """Отменяет заявку только если она принадлежит указанному пользователю.
    Защита от IDOR: раньше отмена работала по одному booking_id без проверки
    владельца, что позволяло отменить чужую запись, зная/подобрав id.

    Дополнительно решает, положен ли возврат оплаты: если до начала игры
    оставалось больше 12 часов И по брони есть подтверждённый платёж, этот
    платёж помечается статусом 'возврат' (новая запись не создаётся —
    история и так видна по created_at/updated-статусу одной записи).
    ">12 часов" считается в SQL через (game_date + game_time) -
    LOCAL_NOW (APP_TIMEZONE), тем же способом, что и статус игр —
    чтобы не разойтись с остальной логикой из-за разных представлений о
    часовом поясе между Python-процессом бота и БД.

    Всё выполняется в одной транзакции с FOR UPDATE (бронь и платёж), чтобы
    избежать гонки с параллельным подтверждением оплаты в CRM.

    Возвращает dict:
        {"status": "not_found"} — брони не существует
        {"status": "forbidden"} — бронь принадлежит другому пользователю
        {"status": "payment_pending_confirm"} — игрок нажал «Я оплатил»,
            админ ещё не подтвердил: отмена запрещена
        {"status": "ok",
         "refund_eligible": bool,   # возврат реально оформлен (>12ч И была подтверждённая оплата)
         "refund_window": bool,     # >12ч до игры, НЕЗАВИСИМО от того, была ли оплата
         "had_payment": bool,       # у брони есть подтверждённый платёж
         "game": dict | None, "payment": dict | None}
        payment — обновлённая (со статусом 'возврат') запись, если возврат
        оформлен, иначе None. refund_window/had_payment нужны боту, чтобы
        показать игроку корректное сообщение и при отмене игры МЕНЕЕ чем за
        12 часов («оплата не возвращается») — это сообщение имеет смысл
        показывать только если оплата была подтверждена (had_payment).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            booking = await conn.fetchrow(
                "SELECT * FROM bookings WHERE id = $1 FOR UPDATE", booking_id
            )
            if booking is None:
                return {"status": "not_found"}
            if booking["user_id"] != user_id:
                return {"status": "forbidden"}

            game = await conn.fetchrow(
                f"""
                SELECT *,
                       ((game_date + game_time) - {_LOCAL_NOW_EXPR}) > INTERVAL '12 hours' AS refund_window,
                       (game_date + game_time) >= {_LOCAL_NOW_EXPR} AS still_upcoming
                FROM games WHERE id = $1
                """,
                booking["game_id"],
            )
            if game is None:
                return {"status": "not_found"}
            if not game["still_upcoming"] or game.get("underfill_cancelled"):
                return {"status": "too_late", "game": _to_dict(game)}
            refund_window = bool(game["refund_window"])

            # Все платежи брони: после докупки может быть несколько строк
            # (старый «подтверждена» + новый «ожидает»).
            all_payments = await conn.fetch(
                """SELECT * FROM payments
                   WHERE booking_id = $1
                   FOR UPDATE""",
                booking_id,
            )
            if any(
                p["status"] == "ожидает" and p["player_notified_at"] is not None
                for p in all_payments
            ):
                return {"status": "payment_pending_confirm"}

            confirmed_payments = [
                p for p in all_payments if p["status"] == "подтверждена"
            ]
            had_payment = bool(confirmed_payments)
            refund_eligible = refund_window and had_payment
            admin_notify_message_id = booking["admin_notify_message_id"]
            admin_extra_notify_message_id = booking.get("admin_extra_notify_message_id")
            payment_deleted = False

            await conn.execute(
                """UPDATE bookings
                   SET status = 'отменена',
                       admin_notify_message_id = NULL,
                       admin_extra_notify_message_id = NULL
                   WHERE id = $1""",
                booking_id,
            )

            payment_result = None
            if refund_eligible:
                for p in confirmed_payments:
                    payment_result = await conn.fetchrow(
                        """UPDATE payments
                              SET status = 'возврат',
                                  admin_attention_at = NOW()
                            WHERE id = $1
                            RETURNING *""",
                        p["id"],
                    )

            # Удаляем только неоплаченные «ожидает». Оплаченные, но ещё не
            # подтверждённые админом (player_notified_at) сюда не попадают —
            # выше уже payment_pending_confirm.
            deleted = await conn.execute(
                """DELETE FROM payments
                   WHERE booking_id = $1
                     AND status = 'ожидает'
                     AND player_notified_at IS NULL""",
                booking_id,
            )
            try:
                payment_deleted = int(str(deleted).split()[-1]) > 0
            except (ValueError, IndexError):
                payment_deleted = True

            if refund_eligible:
                try:
                    import database as db_sync
                    db_sync.clear_badge_cache()
                except Exception:
                    pass

            return {
                "status": "ok",
                "refund_eligible": refund_eligible,
                "refund_window": refund_window,
                "had_payment": bool(had_payment),
                "payment_deleted": payment_deleted,
                "admin_notify_message_id": admin_notify_message_id,
                "admin_extra_notify_message_id": admin_extra_notify_message_id,
                "game": _to_dict(game),
                "payment": _to_dict(payment_result) if payment_result else None,
            }


async def set_booking_admin_notify_message(booking_id: int, message_id: int) -> None:
    """Сохраняет message_id уведомления админу о новой записи — чтобы при
    отмене до оплаты можно было удалить это сообщение в Telegram."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE bookings SET admin_notify_message_id = $1 WHERE id = $2",
        message_id, booking_id,
    )


async def set_booking_admin_extra_notify_message(booking_id: int, message_id: int) -> None:
    """message_id уведомления «Докупка мест» — удаляется при отмене доплаты."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE bookings SET admin_extra_notify_message_id = $1 WHERE id = $2",
        message_id, booking_id,
    )


async def clear_booking_admin_extra_notify_message(booking_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE bookings SET admin_extra_notify_message_id = NULL WHERE id = $1",
        booking_id,
    )


async def log_action(
    action: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    description: Optional[str] = None,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    details: Optional[str] = None,
) -> None:
    """Асинхронный аналог database.log_action (используется CRM) — нужен,
    чтобы бот тоже мог писать в общий журнал admin_logs (например, при
    отмене брони пользователем с возвратом оплаты), не блокируя event loop
    синхронным psycopg2-вызовом."""
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO admin_logs (action, entity_type, entity_id, description, old_value, new_value, details)
           VALUES ($1, $2, $3, $4, $5, $6, $7)""",
        action, entity_type, entity_id, description, old_value, new_value, details,
    )


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


async def get_or_create_pending_payment(booking_id: int, amount: float) -> Optional[dict]:
    """Идемпотентно: один открытый неоплаченный «ожидает» на бронь
    (player_notified_at IS NULL). Уже оплаченные, но ещё не подтверждённые
    админом счета сюда не подмешиваем."""
    pool = await get_pool()
    existing = await pool.fetchrow(
        """SELECT * FROM payments
           WHERE booking_id = $1
             AND status = 'ожидает'
             AND player_notified_at IS NULL
           ORDER BY id DESC LIMIT 1""",
        booking_id,
    )
    if existing is not None:
        return _to_dict(existing)
    try:
        return await create_payment(booking_id, amount)
    except Exception as e:
        # Гонка двух insert'ов — читаем снова.
        logger.warning("get_or_create_pending_payment race booking=%s: %s", booking_id, e)
        existing = await pool.fetchrow(
            """SELECT * FROM payments
               WHERE booking_id = $1
                 AND status = 'ожидает'
                 AND player_notified_at IS NULL
               ORDER BY id DESC LIMIT 1""",
            booking_id,
        )
        return _to_dict(existing) if existing else None


async def attach_provider_payment(
    payment_id: int,
    provider_payment_id: str,
    confirmation_url: str,
    method: str = "yookassa",
) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE payments
           SET provider_payment_id = $1,
               confirmation_url = $2,
               method = COALESCE(method, $3)
           WHERE id = $4 AND status = 'ожидает'
           RETURNING *""",
        provider_payment_id, confirmation_url, method, payment_id,
    )
    return _to_dict(row) if row else None


async def get_payment_by_id(payment_id: int) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM payments WHERE id = $1", payment_id)
    return _to_dict(row)


async def get_payment_by_provider_id(provider_payment_id: str) -> Optional[dict]:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM payments WHERE provider_payment_id = $1",
        provider_payment_id,
    )
    return _to_dict(row) if row else None


async def set_payment_method(payment_id: int, method: str) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE payments SET method = $1 WHERE id = $2", method, payment_id)


async def get_payment_for_user(payment_id: int, user_id: int) -> Optional[dict]:
    """Платёж только если он принадлежит заявке этого user_id — защита от
    IDOR по угаданному payment_id в callback_data бота."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        SELECT p.*, b.user_id AS booking_user_id, b.id AS booking_id_ref, b.status AS booking_status
        FROM payments p
        JOIN bookings b ON b.id = p.booking_id
        WHERE p.id = $1 AND b.user_id = $2
        """,
        payment_id,
        user_id,
    )
    return _to_dict(row)


async def set_payment_method_owned(payment_id: int, user_id: int, method: str) -> Optional[dict]:
    """Меняет способ оплаты только для платежа владельца. None — нет доступа."""
    payment = await get_payment_for_user(payment_id, user_id)
    if not payment:
        return None
    await set_payment_method(payment_id, method)
    payment["method"] = method
    return payment


async def mark_payment_notified(payment_id: int) -> Optional[dict]:
    """Игрок нажал «✅ Я оплатил» / оплатил через PayMaster — атомарно и
    идемпотентно: только статус «ожидает» и только если ещё не уведомлял.
    Именно с этого момента в CRM загорается бейдж «+N» у «Оплаты»."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE payments
              SET player_notified_at = NOW(),
                  admin_attention_at = NOW()
            WHERE id = $1 AND status = 'ожидает' AND player_notified_at IS NULL
            RETURNING *""",
        payment_id,
    )
    result = _to_dict(row) if row else None
    if result is not None:
        # CRM крутится в том же процессе (Render) — сбросить кэш бейджей.
        try:
            import database as db_sync
            db_sync.clear_badge_cache()
        except Exception:
            pass
    return result


async def mark_payment_notified_owned(payment_id: int, user_id: int) -> Optional[dict]:
    """«Я оплатил» только для своего платежа.
    Если уже уведомлял — вернёт payment с флагом _already_notified (без спама админу)."""
    payment = await get_payment_for_user(payment_id, user_id)
    if not payment:
        return None
    if payment.get("status") != "ожидает":
        return None
    if payment.get("player_notified_at") is not None:
        payment["_already_notified"] = True
        return payment
    updated = await mark_payment_notified(payment_id)
    if not updated:
        payment["_already_notified"] = True
        return payment
    return updated


async def confirm_payment(payment_id: int) -> Optional[dict]:
    """Атомарно подтверждает только «ожидает» у активной (не отменённой) заявки."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE payments AS p
           SET status = 'подтверждена',
               player_notified_at = COALESCE(p.player_notified_at, NOW())
           FROM bookings AS b
           WHERE p.id = $1
             AND p.booking_id = b.id
             AND p.status = 'ожидает'
             AND b.status != 'отменена'
           RETURNING p.*""",
        payment_id,
    )
    return _to_dict(row) if row else None


async def register_provider_payment_awaiting_admin(
    provider_payment_id: str,
    expected_amount: Optional[float] = None,
    payment_method: Optional[str] = None,
) -> dict:
    """Деньги пришли у провайдера — статус остаётся «ожидает»,
    ставим player_notified_at; подтверждает админ в CRM.

    Возвращает {"status": "ok"|"already"|"already_notified"|"not_found"|
                "mismatch"|"forbidden", "payment": ...}
    """
    payment_dict = await get_payment_by_provider_id(provider_payment_id)
    if payment_dict is None:
        return {"status": "not_found", "payment": None}
    if payment_dict.get("status") == "подтверждена":
        return {"status": "already", "payment": payment_dict}
    if payment_dict.get("status") != "ожидает":
        return {"status": "forbidden", "payment": payment_dict}
    if expected_amount is not None:
        if abs(float(payment_dict["amount"]) - float(expected_amount)) > 0.009:
            return {"status": "mismatch", "payment": payment_dict}
    if payment_method:
        await set_payment_method(int(payment_dict["id"]), payment_method)
    if payment_dict.get("player_notified_at") is not None:
        return {"status": "already_notified", "payment": payment_dict}
    updated = await mark_payment_notified(int(payment_dict["id"]))
    if not updated:
        again = await get_payment_by_id(int(payment_dict["id"]))
        if again and again.get("player_notified_at") is not None:
            return {"status": "already_notified", "payment": again}
        return {"status": "forbidden", "payment": payment_dict}
    return {"status": "ok", "payment": updated}


async def confirm_payment_by_provider_id(
    provider_payment_id: str,
    expected_amount: Optional[float] = None,
) -> dict:
    """Обратная совместимость: больше не автоподтверждает, а ждёт админа."""
    return await register_provider_payment_awaiting_admin(
        provider_payment_id,
        expected_amount=expected_amount,
    )


async def confirm_payment_owned(payment_id: int, user_id: int, amount_kopecks: int) -> dict:
    """Автоподтверждение Telegram Payments: владелец + сумма + статус «ожидает».

    Возвращает {"status": "ok"|"forbidden"|"mismatch"|"already"|"not_found"}."""
    payment = await get_payment_for_user(payment_id, user_id)
    if not payment:
        return {"status": "not_found"}
    if payment.get("status") == "подтверждена":
        return {"status": "already", "payment": payment}
    expected = int(round(float(payment["amount"]) * 100))
    if expected != int(amount_kopecks):
        return {"status": "mismatch", "payment": payment}
    if payment.get("status") != "ожидает":
        return {"status": "forbidden", "payment": payment}
    updated = await confirm_payment(payment_id)
    if not updated:
        return {"status": "forbidden", "payment": payment}
    return {"status": "ok", "payment": updated}


# ---------------------------------------------------------------------------
# STATISTICS
# ---------------------------------------------------------------------------

async def get_user_statistics(user_id: int) -> dict:
    """Статистика игрока — раньше требовала 4 отдельных запроса,
    теперь один запрос с условными агрегатами (FILTER)."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE b.status = 'отменена') AS cancelled,
            COUNT(*) FILTER (
                WHERE b.status != 'отменена'
                  AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
                  AND (g.game_date + g.game_time) <= {_LOCAL_NOW_EXPR}
            ) AS attended,
            (
                SELECT COUNT(DISTINCT b2.id)
                FROM bookings b2
                JOIN payments p ON p.booking_id = b2.id
                WHERE b2.user_id = $1 AND p.status = 'подтверждена'
            ) AS paid
        FROM bookings b
        LEFT JOIN games g ON g.id = b.game_id
        WHERE b.user_id = $1
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
