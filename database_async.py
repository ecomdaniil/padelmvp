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
        SELECT g.*, LEAST(
            COALESCE(bk.taken, 0) + COALESCE(g.booked_places, 0), g.total_slots
        ) AS taken
        FROM games g
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
    """Игры с недобором состава в заданном окне до старта (Москва)."""
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
                return {"status": "duplicate", "booking": _to_dict(existing), "game": game_dict}

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
            # (даже если их > MAX_SLOTS_PER_BOOKING).
            if timing["within_last_hour"]:
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
            new_taken = effective_taken + slots_count
            return {
                "status": "ok",
                "booking": _to_dict(booking),
                "game": game_dict,
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
    return {
        "game": data,
        "taken": taken,
        "total_slots": total,
        "free_slots": free_slots,
        "within_last_hour": bool(data.get("within_last_hour")),
    }


async def get_active_bookings_for_user(user_id: int) -> list:
    """Только предстоящие (ещё не начавшиеся) незакрытые записи."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT b.*, g.game_date, g.game_time, g.location, g.price, g.total_slots
        FROM bookings b
        JOIN games g ON g.id = b.game_id
        WHERE b.user_id = $1
          AND b.status != 'отменена'
          AND COALESCE(g.underfill_cancelled, FALSE) = FALSE
          AND (g.game_date + g.game_time) >= {_LOCAL_NOW_EXPR}
        ORDER BY g.game_date, g.game_time
        """,
        user_id,
    )
    return _to_dict_list(rows)


async def get_past_bookings_for_user(user_id: int) -> list:
    """Сыгранные / уже прошедшие игры (не отменённые записи)."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT b.*, g.game_date, g.game_time, g.location, g.price, g.total_slots
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
                payment = await conn.fetchrow(
                    """SELECT id, amount, status FROM payments
                       WHERE booking_id = $1 ORDER BY id DESC LIMIT 1 FOR UPDATE""",
                    row["booking_id"],
                )
                await conn.execute(
                    "UPDATE bookings SET status = 'отменена' WHERE id = $1",
                    row["booking_id"],
                )
                refunded = False
                amount = None
                if payment is not None and payment["status"] == "подтверждена":
                    await conn.execute(
                        "UPDATE payments SET status = 'возврат' WHERE id = $1",
                        payment["id"],
                    )
                    refunded = True
                    amount = float(payment["amount"]) if payment["amount"] is not None else None
                elif payment is not None and payment["status"] == "ожидает":
                    await conn.execute(
                        "DELETE FROM payments WHERE id = $1 AND status = 'ожидает'",
                        payment["id"],
                    )
                cancelled.append({
                    "telegram_id": row["telegram_id"],
                    "name": row["name"],
                    "booking_id": row["booking_id"],
                    "refunded": refunded,
                    "amount": amount,
                })

            await conn.execute(
                """UPDATE games
                   SET underfill_cancelled = TRUE, underfill_warn_3h_sent = TRUE
                   WHERE id = $1""",
                game_id,
            )
            game_dict = _to_dict(game)
            game_dict["underfill_cancelled"] = True
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
    payment = await pool.fetchrow(
        """
        SELECT * FROM payments
        WHERE booking_id = $1
        ORDER BY id DESC
        LIMIT 1
        """,
        booking_id,
    )
    payment_dict = _to_dict(payment)
    pending_confirm = bool(
        payment_dict
        and payment_dict.get("status") == "ожидает"
        and payment_dict.get("player_notified_at") is not None
    )
    had_confirmed = bool(payment_dict and payment_dict.get("status") == "подтверждена")
    return {
        "status": "ok",
        "refund_window": bool(game["refund_window"]),
        "payment_pending_confirm": pending_confirm,
        "had_confirmed_payment": had_confirmed,
        "game": _to_dict(game),
        "payment": payment_dict,
        "booking": _to_dict(booking),
    }


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

            latest_payment = await conn.fetchrow(
                """SELECT * FROM payments
                   WHERE booking_id = $1
                   ORDER BY id DESC LIMIT 1 FOR UPDATE""",
                booking_id,
            )
            # Игрок уже нажал «Я оплатил», админ ещё не подтвердил — отмена
            # запрещена, пока статус не станет «подтверждена» (или не сменится
            # иначе в CRM). Иначе можно было бы отменить бронь «между» оплатой
            # и проверкой администратора.
            if (
                latest_payment is not None
                and latest_payment["status"] == "ожидает"
                and latest_payment["player_notified_at"] is not None
            ):
                return {"status": "payment_pending_confirm"}

            confirmed_payment = (
                latest_payment
                if latest_payment is not None and latest_payment["status"] == "подтверждена"
                else None
            )
            # «Была оплата» для текстов бота — только подтверждённая. Платёж
            # «ожидает» при отмене до оплаты удаляем (см. ниже) и в CRM он
            # больше не должен светиться в разделе «Оплаты».
            had_payment = confirmed_payment is not None

            refund_eligible = refund_window and confirmed_payment is not None
            admin_notify_message_id = booking["admin_notify_message_id"]
            payment_deleted = False

            await conn.execute(
                "UPDATE bookings SET status = 'отменена' WHERE id = $1", booking_id
            )

            payment_result = None
            if refund_eligible:
                payment_result = await conn.fetchrow(
                    "UPDATE payments SET status = 'возврат' WHERE id = $1 RETURNING *",
                    confirmed_payment["id"],
                )
            else:
                # Отмена до оплаты (или без подтверждённого платежа) — удаляем
                # «ожидающие» платежи, чтобы они исчезли из CRM «Оплаты».
                deleted = await conn.execute(
                    "DELETE FROM payments WHERE booking_id = $1 AND status = 'ожидает'",
                    booking_id,
                )
                # asyncpg execute возвращает строку вида "DELETE N"
                try:
                    payment_deleted = int(str(deleted).split()[-1]) > 0
                except (ValueError, IndexError):
                    payment_deleted = True

            return {
                "status": "ok",
                "refund_eligible": refund_eligible,
                "refund_window": refund_window,
                "had_payment": bool(had_payment),
                "payment_deleted": payment_deleted,
                "admin_notify_message_id": admin_notify_message_id,
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
    """Игрок нажал «✅ Я оплатил» — атомарно и идемпотентно: только статус
    «ожидает» и только если ещё не уведомлял. Возвращает обновлённую
    строку или None (уже уведомлён / другой статус)."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE payments SET player_notified_at = NOW()
           WHERE id = $1 AND status = 'ожидает' AND player_notified_at IS NULL
           RETURNING *""",
        payment_id,
    )
    return _to_dict(row) if row else None


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
    """Атомарно подтверждает только «ожидает». None — статус уже другой."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """UPDATE payments SET status = 'подтверждена',
               player_notified_at = COALESCE(player_notified_at, NOW())
           WHERE id = $1 AND status = 'ожидает' RETURNING *""",
        payment_id,
    )
    return _to_dict(row) if row else None


async def confirm_payment_by_provider_id(
    provider_payment_id: str,
    expected_amount: Optional[float] = None,
) -> dict:
    """Подтверждение из webhook ЮKassa. Идемпотентно.

    Возвращает {"status": "ok"|"already"|"not_found"|"mismatch"|"forbidden", "payment": ...}
    """
    pool = await get_pool()
    payment = await pool.fetchrow(
        "SELECT * FROM payments WHERE provider_payment_id = $1",
        provider_payment_id,
    )
    if payment is None:
        return {"status": "not_found"}
    payment_dict = _to_dict(payment)
    if payment_dict.get("status") == "подтверждена":
        return {"status": "already", "payment": payment_dict}
    if payment_dict.get("status") != "ожидает":
        return {"status": "forbidden", "payment": payment_dict}
    if expected_amount is not None:
        if abs(float(payment_dict["amount"]) - float(expected_amount)) > 0.009:
            return {"status": "mismatch", "payment": payment_dict}
    updated = await confirm_payment(int(payment_dict["id"]))
    if not updated:
        # гонка с CRM
        again = await get_payment_by_id(int(payment_dict["id"]))
        if again and again.get("status") == "подтверждена":
            return {"status": "already", "payment": again}
        return {"status": "forbidden", "payment": payment_dict}
    return {"status": "ok", "payment": updated}


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
