"""
app.py
------
CRM (веб-панель администратора) на Flask.

Что умеет:
- Вход по логину/паролю (хранятся в .env)
- Просмотр и создание/редактирование игр
- Просмотр заявок (bookings), изменение статуса
- Подтверждение оплат
- Просмотр отзывов
- Выгрузка отчёта в Excel

Запуск (когда виртуальное окружение активировано):
    python app.py
Затем открой в браузере: http://127.0.0.1:5000
"""

import hashlib
import html
import io
import json
import logging
import os
import secrets
import urllib.request
from datetime import date, datetime, time as time_type, timedelta
from decimal import Decimal
from functools import wraps

import pytz
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, send_file, jsonify
)
from flask_compress import Compress
from flask_wtf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from openpyxl import Workbook
from werkzeug.security import check_password_hash

import cache
import database as db

import threading
import asyncio
import time
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from aiogram.fsm.storage.memory import MemoryStorage

load_dotenv()

# Render (и похожие PaaS) задают RENDER / RENDER_EXTERNAL_URL.
_IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_EXTERNAL_URL"))
if _IS_RENDER:
    os.environ.setdefault("SESSION_COOKIE_SECURE", "1")
    os.environ.setdefault("DB_POOL_MIN_SIZE", "1")
    os.environ.setdefault("DB_POOL_MAX_SIZE", "4")
    os.environ.setdefault("ASYNC_DB_POOL_MIN_SIZE", "1")
    os.environ.setdefault("ASYNC_DB_POOL_MAX_SIZE", "3")
    # Бот по умолчанию включён, но можно выключить RUN_BOT_IN_BACKGROUND=0
    # на Render Dashboard, если нужен только CRM (восстановление после OOM).
    os.environ.setdefault("RUN_BOT_IN_BACKGROUND", "1")
    # Дать gunicorn ответить на /health до тяжёлого старта aiogram+asyncpg —
    # иначе Render health check / cron висят на белом экране.
    os.environ.setdefault("BOT_START_DELAY_SECONDS", "25")
    if not (os.getenv("WEBHOOK_URL") or "").strip():
        _external = (os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
        if _external:
            os.environ["WEBHOOK_URL"] = _external

# По умолчанию в логах остаются только ошибки — уровень можно поднять через
# .env (LOG_LEVEL=INFO/DEBUG), например для отладки на staging.
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.ERROR))
logger = logging.getLogger(__name__)

if _IS_RENDER:
    logger.warning(
        "Render: RUN_BOT_IN_BACKGROUND=%s WEBHOOK_URL=%s",
        os.getenv("RUN_BOT_IN_BACKGROUND"),
        os.getenv("WEBHOOK_URL") or "(missing)",
    )

app = Flask(__name__)

FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")
if not FLASK_SECRET_KEY:
    raise RuntimeError(
        "Не найден FLASK_SECRET_KEY в .env. "
        "Сгенерируйте случайный ключ и добавьте его в .env перед запуском."
    )
app.secret_key = FLASK_SECRET_KEY

# Cookie сессии: недоступны из JS, не передаются на сторонние сайты.
# SESSION_COOKIE_SECURE по умолчанию выключен, чтобы не сломать локальный
# запуск по HTTP — в production (за HTTPS) обязательно включите
# SESSION_COOKIE_SECURE=1 в .env.
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "0").lower() in {"1", "true", "yes"}
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
    hours=int(os.getenv("SESSION_LIFETIME_HOURS", "12"))
)
app.config["SESSION_REFRESH_EACH_REQUEST"] = True
# Debug только по явному флагу — иначе в HTML может утечь текст исключения.
app.config["DEBUG"] = os.getenv("FLASK_DEBUG", "0").lower() in {"1", "true", "yes"}
app.debug = app.config["DEBUG"]

# Кэширование статики в браузере (сек). 1 год для /static, т.к. имена файлов
# можно версионировать при изменениях; по умолчанию — сутки.
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = int(os.getenv("STATIC_CACHE_SECONDS", "86400"))

# Gzip/Brotli-компрессия ответов (в т.ч. статики и HTML-страниц со списками).
app.config["COMPRESS_MIMETYPES"] = [
    "text/html", "text/css", "text/xml", "application/json",
    "application/javascript", "text/javascript",
]
Compress(app)

# CSRF-защита всех форм (PIP: Flask-WTF). Токен подставляется в шаблонах
# через {{ csrf_token() }} и автоматически проверяется на каждый POST.
csrf = CSRFProtect(app)

# Rate limiting: не более 10 запросов в секунду с одного IP. Отдельно более
# строгий лимит применяется к /login, чтобы затруднить брутфорс пароля.
# В production задайте REDIS_URL — иначе лимиты in-memory и сбрасываются
# при рестарте / не шарятся между воркерами.
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=[f"{os.getenv('CRM_RATE_LIMIT_PER_SECOND', '10')} per second"],
    storage_uri=os.getenv("REDIS_URL", "memory://"),
)

ADMIN_LOGIN = os.getenv("ADMIN_LOGIN")
# Предпочтительно ADMIN_PASSWORD_HASH (werkzeug/scrypt/pbkdf2). ADMIN_PASSWORD
# оставлен только для обратной совместимости локальных .env — в проде хеш.
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH") or ""
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD") or ""
if not ADMIN_LOGIN or (not ADMIN_PASSWORD_HASH and not ADMIN_PASSWORD):
    raise RuntimeError(
        "Не найдены ADMIN_LOGIN и пароль в .env. "
        "Задайте ADMIN_PASSWORD_HASH "
        "(python -c \"from werkzeug.security import generate_password_hash; "
        "print(generate_password_hash('ваш_пароль'))\") "
        "или временно ADMIN_PASSWORD для локальной разработки."
    )
if ADMIN_PASSWORD and not ADMIN_PASSWORD_HASH:
    logger.warning(
        "ADMIN_PASSWORD задан в открытом виде. Перейдите на ADMIN_PASSWORD_HASH "
        "и удалите ADMIN_PASSWORD из .env."
    )

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
# Секрет для X-Telegram-Bot-Api-Secret-Token. Если не задан явно — стабильный
# дериват от FLASK_SECRET_KEY (чтобы set_webhook и проверка совпадали без
# обязательной новой переменной). В проде лучше задать WEBHOOK_SECRET_TOKEN
# отдельно и ротировать при утечке.
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN") or hashlib.sha256(
    f"tg-webhook:{FLASK_SECRET_KEY}".encode("utf-8")
).hexdigest()

BOOKING_STATUSES = frozenset({"новая", "подтверждена", "отменена", "посещена"})


def _verify_admin_password(password_value: str) -> bool:
    """Проверка пароля админа: сначала хеш, иначе (legacy) plaintext."""
    if not password_value:
        return False
    if ADMIN_PASSWORD_HASH:
        try:
            return check_password_hash(ADMIN_PASSWORD_HASH, password_value)
        except (ValueError, TypeError):
            logger.error("ADMIN_PASSWORD_HASH имеет неверный формат")
            return False
    return bool(ADMIN_PASSWORD) and secrets.compare_digest(password_value, ADMIN_PASSWORD)


@app.after_request
def set_security_headers(response):
    """Базовые security-заголовки для всех ответов CRM."""
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    # Inline CSS/JS в шаблонах CRM — разрешаем 'unsafe-inline'; внешние
    # скрипты/стили не подключаем.
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'",
    )
    return response


def send_telegram_message(chat_id, text: str) -> bool:
    """Отправляет сообщение игроку прямо из CRM через HTTPS Bot API — в
    обход bot_instance/bot_loop (см. run_bot()) специально: те существуют
    только если RUN_BOT_IN_BACKGROUND=1 и бот уже успел стартовать в этом же
    процессе, а прямой вызов Bot API работает всегда, независимо от того,
    как и где запущен бот (в этом же процессе, отдельным сервисом,
    webhook/long polling). Синхронный urllib с коротким таймаутом — здесь
    не нужен отдельный HTTP-клиент только для одного вызова в секунду.
    Ошибка не должна ронять действие админа (подтверждение оплаты и т.п.),
    поэтому она только логируется, исключение наружу не уходит."""
    if not BOT_TOKEN or not chat_id:
        return False
    try:
        payload = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error("Не удалось отправить сообщение игроку (chat_id=%s): %s", chat_id, e)
        return False

DEFAULT_PAGE_SIZE = int(os.getenv("CRM_PAGE_SIZE", "20"))

# БД хранит created_at как naive-время в UTC (сессия Postgres — UTC).
# В CRM и планировщиках везде показываем/считаем московское время клуба.
APP_TIMEZONE = pytz.timezone("Europe/Moscow")

# Сколько дней хранить старые брони/записи журнала перед автоочисткой —
# см. _run_cleanup_job() и start_cleanup_scheduler() ближе к концу файла.
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "60"))


def _to_local_dt(value):
    """Переводит naive UTC datetime (как приходит из БД) в APP_TIMEZONE.
    Если объект уже tz-aware — просто конвертирует. None передаётся как есть."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = pytz.utc.localize(value)
    return value.astimezone(APP_TIMEZONE)


@app.template_filter("local_dt")
def local_dt_filter(value, fmt="%d.%m.%Y %H:%M"):
    """Jinja-фильтр: {{ log.created_at | local_dt }} — форматирует datetime
    из БД (UTC) в локальном времени администратора."""
    local = _to_local_dt(value)
    return local.strftime(fmt) if local else "—"


_RU_WEEKDAYS = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
_RU_MONTHS = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def _dashboard_greeting() -> dict:
    """Приветствие и дата для шапки главной страницы CRM — время дня и
    дата по Москве (Europe/Moscow), а не UTC сервера/БД."""
    now = datetime.now(APP_TIMEZONE)
    hour = now.hour
    if 5 <= hour < 12:
        greeting = "Доброе утро"
    elif 12 <= hour < 18:
        greeting = "Добрый день"
    elif 18 <= hour < 23:
        greeting = "Добрый вечер"
    else:
        greeting = "Доброй ночи"
    date_str = f"{now.day} {_RU_MONTHS[now.month - 1]}, {_RU_WEEKDAYS[now.weekday()]}"
    return {"greeting": greeting, "date_str": date_str}

# Тот же набор уровней, что и в боте (см. bot.py: VALID_LEVELS) — продублирован
# здесь, чтобы не тянуть в CRM тяжёлые импорты aiogram/бота только за одной
# константой. Если список уровней поменяется — обновите его в обоих местах.
GAME_LEVELS = ["Новичок", "Любитель", "Продвинутый", "Профессионал"]

# Глобальные переменные для бота: единственные Bot/Dispatcher на процесс
# (раньше bot.py создавал свою отдельную пару внутри main(), что приводило
# к путанице — теперь webhook всегда обслуживается вот этими инстансами).
bot_instance = None
dp_instance = None
bot_loop = None
bot_start_error = None
_bot_services_started = False
_bot_services_lock = threading.Lock()


def get_bot_health() -> dict:
    loop_running = bool(bot_loop is not None and getattr(bot_loop, "is_running", lambda: False)())
    ready = bool(bot_instance and dp_instance and loop_running and not bot_start_error)
    return {
        "bot_ready": ready,
        "bot_error": bot_start_error,
        "bot_thread_started": _bot_services_started,
        "bot_loop_running": loop_running,
        "run_bot_flag": os.getenv("RUN_BOT_IN_BACKGROUND"),
        "webhook_url": os.getenv("WEBHOOK_URL") or None,
        "has_bot_token": bool(BOT_TOKEN),
    }


# ---------------------------------------------------------------------------
# Журнал действий администратора (admin_logs)
# ---------------------------------------------------------------------------
#
# Раньше в new_value/old_value писался json.dumps() всей строки сущности —
# в /logs это выглядело как нечитаемый {"game": {"id": 9, ...}}. Теперь
# каждый вызывающий код сам формирует человекочитаемый текст на русском
# (description) — что именно произошло, — а не сырой снимок объекта.
# Переводы кодов action/entity_type для колонок журнала:
ACTION_LABELS = {
    "create": "Создание",
    "update": "Редактирование",
    "delete": "Удаление",
    "update_status": "Изменение статуса",
    "confirm": "Подтверждение оплаты",
    "mark_visited": "Отметка посещения",
    "cleanup": "Автоочистка данных",
    "refund": "Возврат оплаты",
    "confirm_refund": "Оформление возврата",
}
ENTITY_LABELS = {
    "game": "Игра",
    "booking": "Бронирование",
    "payment": "Оплата",
    "club": "Клуб",
    "club_info": "Информация о клубе",
    "system": "Системная задача",
}
app.jinja_env.filters["action_label"] = lambda a: ACTION_LABELS.get(a, a or "—")
app.jinja_env.filters["entity_label"] = lambda e: ENTITY_LABELS.get(e, e) if e else "—"


def _ru_plural(n, one: str, few: str, many: str) -> str:
    """Русское склонение по числу: 1 место / 2 места / 5 мест."""
    n = abs(int(n))
    if n % 100 in (11, 12, 13, 14):
        return many
    last = n % 10
    if last == 1:
        return one
    if 2 <= last <= 4:
        return few
    return many


def _fmt_money(value) -> str:
    return f"{float(value):.0f}"


def _fmt_date(d) -> str:
    return d.strftime("%d.%m.%Y") if d else "—"


def _fmt_time(t) -> str:
    return str(t)[:5] if t else "—"


def _describe_game_diff(old: dict, new: dict) -> str:
    """Список изменённых полей игры в формате "поле изменено с X на Y" —
    используется в описании действия "update"/"game" в журнале."""
    changes = []
    if old.get("game_date") != new.get("game_date"):
        changes.append(f"дата изменена с {_fmt_date(old.get('game_date'))} на {_fmt_date(new.get('game_date'))}")
    if old.get("game_time") != new.get("game_time"):
        changes.append(f"время изменено с {_fmt_time(old.get('game_time'))} на {_fmt_time(new.get('game_time'))}")
    if (old.get("location") or "") != (new.get("location") or ""):
        changes.append(f"место изменено с «{old.get('location') or '—'}» на «{new.get('location') or '—'}»")
    if float(old.get("price") or 0) != float(new.get("price") or 0):
        changes.append(f"цена изменена с {_fmt_money(old.get('price') or 0)} на {_fmt_money(new.get('price') or 0)} руб.")
    if old.get("total_slots") != new.get("total_slots"):
        changes.append(f"количество мест изменено с {old.get('total_slots')} на {new.get('total_slots')}")
    if (old.get("duration_minutes") or 90) != (new.get("duration_minutes") or 90):
        changes.append(
            f"длительность изменена с {old.get('duration_minutes') or 90} "
            f"на {new.get('duration_minutes') or 90} мин."
        )
    if (old.get("level") or "") != (new.get("level") or ""):
        changes.append(f"уровень изменён с «{old.get('level') or '—'}» на «{new.get('level') or '—'}»")
    if (old.get("booked_places") or 0) != (new.get("booked_places") or 0):
        changes.append(
            f"занято мест (вручную) изменено с {old.get('booked_places') or 0} "
            f"на {new.get('booked_places') or 0}"
        )
    return "; ".join(changes) if changes else "без изменений"


def _describe_club_diff(old: dict, new: dict) -> str:
    changes = []
    if old.get("name") != new.get("name"):
        changes.append(f"название изменено с «{old.get('name')}» на «{new.get('name')}»")
    if (old.get("city") or "") != (new.get("city") or ""):
        changes.append(f"город изменён с «{old.get('city') or '—'}» на «{new.get('city') or '—'}»")
    if old.get("address") != new.get("address"):
        changes.append(f"адрес изменён с «{old.get('address')}» на «{new.get('address')}»")
    if old.get("phone") != new.get("phone"):
        changes.append(f"телефон изменён с «{old.get('phone')}» на «{new.get('phone')}»")
    if (old.get("description") or "") != (new.get("description") or ""):
        changes.append("описание изменено")
    return "; ".join(changes) if changes else "без изменений"


def _describe_club_info_diff(old: dict, new: dict) -> str:
    labels = [("name", "название"), ("description", "описание"), ("contact_phone", "телефон"), ("contact_email", "email")]
    changes = []
    for key, label in labels:
        if (old.get(key) or "") != (new.get(key) or ""):
            changes.append(f"{label} изменён(о)")
    return "; ".join(changes) if changes else "без изменений"


def log_admin_action(action, entity_type=None, entity_id=None, description=None, old=None, new=None, details=None):
    """Тонкая обёртка над db.log_action: description — человекочитаемый
    текст на русском ("Игра №9 создана: ...") — именно он показывается в
    журнале (/logs). old/new здесь — только простые строковые значения
    (например статус "ожидает"/"подтверждена"), НЕ целые объекты — полные
    словари сущностей туда больше не передаются, чтобы не плодить JSON.
    Никогда не роняет основной запрос, если запись в журнал вдруг не удалась."""
    try:
        db.log_action(
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            description=description,
            old_value=str(old) if old is not None else None,
            new_value=str(new) if new is not None else None,
            details=details,
        )
    except Exception as e:
        logger.error("Не удалось записать действие в журнал (%s %s#%s): %s", action, entity_type, entity_id, e)


# ---------------------------------------------------------------------------
# Авторизация
# ---------------------------------------------------------------------------

def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapped


# ---------------------------------------------------------------------------
# Бейджи "+N" в шапке меню (Заявки/Оплаты/Отзывы)
# ---------------------------------------------------------------------------
# Идея: в сессии админа храним id последней увиденной заявки/отзыва
# (seen_booking_id/seen_review_id) — бейдж = сколько строк имеют id больше
# сохранённого. Для оплат — не id, а seen_payment_notified_at (timestamp),
# так как "новизна" оплаты для бейджа определяется моментом, когда игрок
# нажал "✅ Я оплатил" (может произойти позже создания более новых платежей),
# а не порядком id. Как только админ открывает раздел, seen_* подтягивается
# до текущего максимума, и бейдж пропадает.

_UNSET = object()


def _mark_section_seen(booking_id=None, payment_notified_at=_UNSET, review_id=None) -> None:
    if booking_id is not None:
        session["seen_booking_id"] = booking_id
    if payment_notified_at is not _UNSET:
        # Храним как ISO-строку — datetime не кладётся в сессию как есть.
        # None допустим (значит: ни одного платежа игрок ещё не подтверждал).
        session["seen_payment_notified_at"] = (
            payment_notified_at.isoformat() if payment_notified_at else None
        )
    if review_id is not None:
        session["seen_review_id"] = review_id


def _mark_all_sections_seen() -> None:
    try:
        marker = db.get_latest_activity_marker()
    except Exception as e:
        logger.error("Не удалось инициализировать бейджи после входа: %s", e)
        return
    _mark_section_seen(
        booking_id=marker["max_booking_id"],
        # seen_payment_notified_at хранит момент, когда игрок в последний раз
        # (до этого визита) нажимал "✅ Я оплатил", а не id последнего
        # созданного платежа — см. count_new_since в database.py.
        payment_notified_at=marker["last_payment_notified_at"],
        review_id=marker["max_review_id"],
    )


@app.context_processor
def inject_nav_badges():
    """Бейджи "+N" в шапке. На рендере страницы НЕ ходим в БД — только
    in-memory кэш (его наполняет /api/activity). Иначе каждый клик по меню
    = лишний ~1с round-trip к Neon сверх запроса самой страницы."""
    if not session.get("logged_in"):
        return {}
    try:
        counts = db.peek_badge_counts(
            session.get("seen_booking_id", 0),
            session.get("seen_payment_notified_at"),
            session.get("seen_review_id", 0),
        )
    except Exception as e:
        logger.error("Не удалось прочитать бейджи меню: %s", e)
        counts = None
    if not counts:
        return {"nav_badges": {"bookings": 0, "payments": 0, "reviews": 0}}
    return {"nav_badges": {
        "bookings": counts["new_bookings"],
        "payments": counts["new_payments"],
        "reviews": counts["new_reviews"],
    }}


@app.route("/login", methods=["GET", "POST"])
@limiter.limit(os.getenv("LOGIN_RATE_LIMIT", "5 per minute"))
def login():
    if session.get("logged_in"):
        return redirect(url_for("index"))
    if request.method == "POST":
        login_value = request.form.get("login", "")
        password_value = request.form.get("password", "")

        # Логин — compare_digest (timing-safe). Пароль — хеш (или legacy plaintext).
        valid_login = bool(ADMIN_LOGIN) and secrets.compare_digest(login_value, ADMIN_LOGIN)
        valid_password = _verify_admin_password(password_value)

        if valid_login and valid_password:
            # session.clear() сбрасывает старый session id → защита от fixation.
            session.clear()
            session.permanent = True
            session["logged_in"] = True
            # Отмечаем всё, что накопилось ДО этого входа, как уже "увиденное" —
            # иначе сразу после входа бейджи "+N" показали бы весь объём старых
            # заявок/оплат/отзывов, а не только новые.
            _mark_all_sections_seen()
            logger.info("Успешный вход в CRM с IP %s", get_remote_address())
            return redirect(url_for("games_list"))
        else:
            logger.warning("Неудачная попытка входа в CRM с IP %s", get_remote_address())
            flash("Неверный логин или пароль")

    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.errorhandler(429)
def ratelimit_handler(err):
    """Превышен rate limit — единый ответ без деталей лимитера наружу."""
    logger.warning("Rate limit 429: %s %s from %s", request.method, request.path, get_remote_address())
    if request.path.startswith("/api/"):
        return {"error": "too_many_requests"}, 429
    flash("Слишком много запросов. Подождите немного и попробуйте снова.")
    return render_template("error.html", message="Слишком много запросов. Попробуйте позже."), 429


@app.errorhandler(Exception)
def handle_unexpected_error(err):
    """Раньше необработанное исключение в любом роуте отдавало голый 500
    (или, в неудачных случаях, вешало страницу до таймаута gunicorn) — для
    админа это выглядело как «сайт упал». Теперь логируем полную трассировку
    для отладки и возвращаем понятную страницу, СЕССИЯ (логин) при этом не
    трогается — обновление страницы просто вернёт админа туда же."""
    from werkzeug.exceptions import HTTPException

    if isinstance(err, HTTPException):
        return err
    logger.exception("Необработанная ошибка при обработке %s %s", request.method, request.path)
    # В production никогда не отдаём текст исключения клиенту.
    try:
        return render_template("error.html", message=None), 500
    except Exception:
        return "Произошла ошибка на сервере. Попробуйте обновить страницу.", 500


@app.route("/")
@login_required
def index():
    # Один запрос + короткий in-memory кэш (см. get_dashboard_summary_cached).
    summary = db.get_dashboard_summary_cached()
    return render_template("dashboard.html", summary=summary, **_dashboard_greeting())


@app.route("/health")
@limiter.exempt
def health_check():
    """Публичный health для Render/uptime-cron. Без БД и без блокировок."""
    payload = {"status": "ok", "webhook_path": WEBHOOK_PATH}
    payload.update(get_bot_health())
    return payload, 200


@app.route("/api/activity")
@login_required
def api_activity():
    """Лёгкий эндпоинт для поллинга с bookings.html/payments.html — фронтенд
    раз в несколько секунд сверяет счётчики с тем, что было при загрузке
    страницы, и если что-то новое появилось (например, бронирование и
    оплата из бота), показывает баннер «Обновить». Одна простая агрегатная
    выборка (см. db.get_latest_activity_marker), не создаёт заметной
    нагрузки даже при частом опросе.

    Дополнительно отдаёт new_bookings/new_payments/new_reviews — сколько
    появилось нового с момента, когда админ последний раз открывал
    соответствующий раздел (см. seen_*_id в сессии) — этим живут бейджи
    "+N" в шапке меню на ЛЮБОЙ странице CRM, без перезагрузки (см.
    startNavBadgesPoll в base.html)."""
    # Один запрос вместо двух (marker + count_new_since) — иначе поллинг
    # каждые ~8с платил двумя round-trip'ами к удалённому Postgres.
    snapshot = db.get_activity_snapshot(
        session.get("seen_booking_id", 0),
        session.get("seen_payment_notified_at"),
        session.get("seen_review_id", 0),
    )
    # datetime из Postgres нельзя отдать в jsonify as-is на всех версиях Flask.
    notified = snapshot.get("last_payment_notified_at")
    if isinstance(notified, datetime):
        snapshot["last_payment_notified_at"] = notified.isoformat()
    return snapshot, 200


def _json_value(value):
    """psycopg2 отдаёт Decimal/date/time/datetime, которые стандартный JSON
    не умеет сериализовать сам — приводим к простым JSON-совместимым типам.
    datetime дополнительно переводится из UTC (как хранится в БД) в
    локальное время администратора — см. _to_local_dt."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _to_local_dt(value).strftime("%d.%m.%Y %H:%M")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    if isinstance(value, time_type):
        return str(value)[:5]
    return value


def _json_row(row):
    if row is None:
        return None
    return {key: _json_value(value) for key, value in dict(row).items()}


@app.route("/api/games/<int:game_id>/details")
@login_required
def api_game_details(game_id):
    """Полные сведения об игре (все поля + список участников с оплатами) —
    используется модальным окном "Подробнее" на карточке игры в /games:
    клик по иконке игры (в отличие от кнопки "Изменить") не переходит на
    другую страницу, а подгружает эти данные через fetch()."""
    details = db.get_game_details(game_id)
    if not details:
        return jsonify({"error": "Игра не найдена"}), 404
    return jsonify({
        "game": _json_row(details["game"]),
        "participants": [_json_row(p) for p in details["participants"]],
    })


# ---------------------------------------------------------------------------
# Игры
# ---------------------------------------------------------------------------

def _game_values_from_form(form):
    """Сырые строковые значения формы (и при GET со старой игрой, и при
    повторном показе формы после ошибки валидации используется один и тот же
    формат — см. _game_values_from_row)."""
    return {
        "game_date": form.get("game_date", "").strip(),
        "game_time": form.get("game_time", "").strip(),
        "city": form.get("city", "").strip(),
        "club_id": form.get("club_id", "").strip(),
        "address": form.get("address", "").strip(),
        "price": form.get("price", "").strip(),
        "total_slots": form.get("total_slots", "").strip(),
        "duration_minutes": form.get("duration_minutes", "").strip(),
        "level": form.get("level", "").strip(),
        "booked_places": form.get("booked_places", "").strip(),
    }


def _game_values_from_row(game):
    """Тот же формат значений, но из строки БД (для GET /games/new и .../edit)."""
    if not game:
        return {
            "game_date": "", "game_time": "", "city": "", "club_id": "",
            "address": "", "price": "", "total_slots": "",
            "duration_minutes": "90", "level": "", "booked_places": "0",
        }
    return {
        "game_date": game["game_date"].isoformat(),
        "game_time": game["game_time"].strftime("%H:%M"),
        "city": game.get("city") or "",
        "club_id": str(game["club_id"]) if game.get("club_id") else "",
        "address": game.get("address") or "",
        "price": str(game["price"]),
        "total_slots": str(game["total_slots"]),
        "duration_minutes": str(game.get("duration_minutes") or 90),
        "level": game.get("level") or "",
        "booked_places": str(game.get("booked_places") or 0),
    }


def _validate_game_values(values, clubs_by_id, actual_taken: int = 0):
    """Валидирует все поля формы игры. Возвращает (errors, parsed) — при
    непустом errors значения в parsed могут быть неполными/некорректными,
    использовать их для сохранения в БД нельзя.

    actual_taken — сколько мест уже занято реальными бронированиями (0 для
    новой игры). booked_places — это ДОПОЛНИТЕЛЬНЫЕ места, занятые мимо бота
    (например, по телефону), поэтому лимит для него — не total_slots, а
    total_slots - actual_taken (то, что реально остаётся)."""
    errors = []
    parsed = {}

    try:
        parsed["game_date"] = datetime.strptime(values["game_date"], "%Y-%m-%d").date()
    except ValueError:
        errors.append("Укажите корректную дату игры.")

    try:
        parsed["game_time"] = datetime.strptime(values["game_time"], "%H:%M").time()
    except ValueError:
        errors.append("Укажите корректное время игры.")

    parsed["club_id"] = None
    if not values["club_id"]:
        errors.append("Выберите клуб — город и адрес подставятся из него.")
    else:
        try:
            club_id_int = int(values["club_id"])
        except ValueError:
            errors.append("Некорректно выбран клуб.")
        else:
            if club_id_int not in clubs_by_id:
                errors.append("Выбранный клуб не найден.")
            else:
                parsed["club_id"] = club_id_int
                club = clubs_by_id[club_id_int]
                # Если поля пустые (JS не сработал) — берём из клуба.
                if not values["city"] and club.get("city"):
                    values["city"] = club["city"]
                if not values["address"] and club.get("address"):
                    values["address"] = club["address"]

    if not values["city"]:
        errors.append("Укажите город (или выберите клуб с заполненным городом).")
    parsed["city"] = (values["city"] or "")[:100]

    if not values["address"]:
        errors.append("Укажите адрес.")
    parsed["address"] = (values["address"] or "")[:255]

    try:
        price = float(values["price"].replace(",", "."))
        if price <= 0:
            errors.append("Цена за место должна быть больше нуля.")
        parsed["price"] = price
    except (ValueError, AttributeError):
        errors.append("Укажите корректную цену за место.")

    try:
        total_slots = int(values["total_slots"])
        if not (1 <= total_slots <= 100):
            errors.append("Количество мест должно быть от 1 до 100.")
        parsed["total_slots"] = total_slots
    except ValueError:
        errors.append("Укажите корректное количество мест (целое число).")

    try:
        booked_places = int(values.get("booked_places") or 0)
        if booked_places < 0:
            errors.append("Занято мест (вручную) не может быть отрицательным.")
        elif "total_slots" in parsed:
            remaining = parsed["total_slots"] - actual_taken
            if booked_places > remaining:
                errors.append(
                    f"Занято мест (вручную) не может превышать оставшиеся свободные места "
                    f"({remaining} из {parsed['total_slots']}, с учётом {actual_taken} уже "
                    f"забронированных через бота)."
                )
        parsed["booked_places"] = booked_places
    except ValueError:
        errors.append("Укажите корректное число занятых мест (целое число).")

    try:
        duration = int(values["duration_minutes"])
        if not (15 <= duration <= 480):
            errors.append("Длительность должна быть от 15 до 480 минут.")
        parsed["duration_minutes"] = duration
    except ValueError:
        errors.append("Укажите корректную длительность в минутах.")

    if values["level"] and values["level"] not in GAME_LEVELS:
        errors.append("Некорректно выбран уровень.")
    parsed["level"] = values["level"] if values["level"] in GAME_LEVELS else None

    return errors, parsed


def _build_game_location(parsed, clubs_by_id):
    """Собирает человекочитаемое location из клуба/города/адреса — его
    продолжают читать бот и старые запросы/шаблоны (см. _migrate_games_table
    в database.py)."""
    club_name = clubs_by_id[parsed["club_id"]]["name"] if parsed.get("club_id") else None
    parts = [p for p in [club_name, parsed["city"], parsed["address"]] if p]
    return ", ".join(parts)


def _safe_date(value: str):
    """Парсит "YYYY-MM-DD" из query-параметра; None, если пусто/некорректно —
    некорректный фильтр молча игнорируется, а не роняет страницу с 500."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _safe_time(value: str):
    """Аналог _safe_date для "HH:MM"."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError:
        return None


@app.route("/games")
@login_required
def games_list():
    page = request.args.get("page", 1, type=int)
    level = request.args.get("level", "")
    city = request.args.get("city", "")
    date_from_raw = request.args.get("date_from", "")
    date_to_raw = request.args.get("date_to", "")
    time_from_raw = request.args.get("time_from", "")
    time_to_raw = request.args.get("time_to", "")
    sort_order = request.args.get("sort", "asc")
    if sort_order not in ("asc", "desc"):
        sort_order = "asc"
    fullness = request.args.get("fullness", "")
    if fullness not in ("full", "available"):
        fullness = ""
    show_past = request.args.get("show_past") == "1"

    result = db.get_games_paginated(
        page=page, per_page=DEFAULT_PAGE_SIZE, level=level, city=city,
        date_from=_safe_date(date_from_raw), date_to=_safe_date(date_to_raw),
        time_from=_safe_time(time_from_raw), time_to=_safe_time(time_to_raw),
        sort_order=sort_order, fullness=fullness, show_past=show_past,
    )
    cities = db.get_distinct_game_cities()
    return render_template(
        "games.html", games=result["items"], pagination=result,
        levels=GAME_LEVELS, selected_level=level,
        cities=cities, selected_city=city,
        date_from=date_from_raw, date_to=date_to_raw,
        time_from=time_from_raw, time_to=time_to_raw,
        sort_order=sort_order, fullness=fullness, show_past=show_past,
    )


@app.route("/games/new", methods=["GET", "POST"])
@login_required
def game_new():
    clubs = db.get_all_clubs()
    clubs_by_id = {c["id"]: c for c in clubs}

    if request.method == "POST":
        values = _game_values_from_form(request.form)
        errors, parsed = _validate_game_values(values, clubs_by_id)
        if errors:
            for e in errors:
                flash(e)
            return render_template(
                "game_form.html", game=None, values=values, clubs=clubs, levels=GAME_LEVELS
            )

        location = _build_game_location(parsed, clubs_by_id)
        game = db.create_game(
            game_date=parsed["game_date"],
            game_time=parsed["game_time"],
            location=location,
            price=parsed["price"],
            total_slots=parsed["total_slots"],
            city=parsed["city"],
            club_id=parsed["club_id"],
            address=parsed["address"],
            duration_minutes=parsed["duration_minutes"],
            level=parsed["level"],
            booked_places=parsed["booked_places"],
        )
        # Бот кэширует список ближайших игр (см. cache.py/bot.py) — без
        # явного сброса здесь новая игра была видна в боте только после
        # истечения TTL кэша или перезапуска процесса бота.
        cache.invalidate_games_cache()
        description = (
            f"Игра №{game['id']} создана: {game['location']}, "
            f"{_fmt_date(game['game_date'])} {_fmt_time(game['game_time'])}, "
            f"{game['total_slots']} {_ru_plural(game['total_slots'], 'место', 'места', 'мест')}, "
            f"{_fmt_money(game['price'])} руб."
        )
        log_admin_action("create", "game", game["id"], description=description)
        flash("Игра создана")
        return redirect(url_for("games_list"))

    return render_template(
        "game_form.html", game=None, values=_game_values_from_row(None),
        clubs=clubs, levels=GAME_LEVELS,
    )


@app.route("/games/<int:game_id>/edit", methods=["GET", "POST"])
@login_required
def game_edit(game_id):
    game = db.get_game_by_id(game_id)
    if not game:
        flash("Игра не найдена")
        return redirect(url_for("games_list"))

    clubs = db.get_all_clubs()
    clubs_by_id = {c["id"]: c for c in clubs}

    if request.method == "POST":
        values = _game_values_from_form(request.form)
        # actual_taken нужен ДО валидации: booked_places — это ДОПОЛНИТЕЛЬНЫЕ
        # места сверх реальных бронирований, поэтому лимит для него зависит
        # от того, сколько мест уже занято через бота.
        actual_taken = db.count_bookings_for_game(game_id)
        errors, parsed = _validate_game_values(values, clubs_by_id, actual_taken=actual_taken)
        if errors:
            for e in errors:
                flash(e)
            return render_template(
                "game_form.html", game=game, values=values, clubs=clubs, levels=GAME_LEVELS,
                actual_taken=actual_taken,
            )

        location = _build_game_location(parsed, clubs_by_id)
        old_snapshot = dict(game)
        updated = db.update_game(
            game_id=game_id,
            game_date=parsed["game_date"],
            game_time=parsed["game_time"],
            location=location,
            price=parsed["price"],
            total_slots=parsed["total_slots"],
            city=parsed["city"],
            club_id=parsed["club_id"],
            address=parsed["address"],
            duration_minutes=parsed["duration_minutes"],
            level=parsed["level"],
            booked_places=parsed["booked_places"],
        )

        cache.invalidate_games_cache()
        description = f"Игра №{game_id} отредактирована: {_describe_game_diff(old_snapshot, dict(updated))}"
        log_admin_action("update", "game", game_id, description=description)
        flash("Игра обновлена")
        return redirect(url_for("games_list"))

    actual_taken = db.count_bookings_for_game(game_id)
    return render_template(
        "game_form.html", game=game, values=_game_values_from_row(game),
        clubs=clubs, levels=GAME_LEVELS, actual_taken=actual_taken,
    )


@app.route("/games/<int:game_id>/delete", methods=["POST"])
@login_required
def game_delete(game_id):
    game = db.get_game_by_id(game_id)
    if not game:
        flash("Игра не найдена")
        return redirect(url_for("games_list"))

    db.delete_game(game_id)
    # Удаление игры каскадно удаляет её заявки и оплаты (ON DELETE CASCADE) —
    # число занятых мест по другим играм не меняется, но кэш списка игр,
    # который видит бот, всё равно нужно сбросить.
    cache.invalidate_games_cache()
    description = (
        f"Игра №{game_id} удалена: {game['location']}, "
        f"{_fmt_date(game['game_date'])} {_fmt_time(game['game_time'])}"
    )
    log_admin_action("delete", "game", game_id, description=description)
    flash("Игра удалена")
    return redirect(url_for("games_list"))


# ---------------------------------------------------------------------------
# Заявки (bookings)
# ---------------------------------------------------------------------------

@app.route("/bookings")
@login_required
def bookings_list():
    search = request.args.get("search", "")
    status_filter = request.args.get("status", "")
    page = request.args.get("page", 1, type=int)
    result = db.get_bookings_paginated(
        search=search, status=status_filter, page=page, per_page=DEFAULT_PAGE_SIZE
    )
    activity = db.get_latest_activity_marker()
    # Открыли раздел — бейдж "+N" у "Заявки" в шапке пропадает.
    _mark_section_seen(booking_id=activity["max_booking_id"])
    return render_template(
        "bookings.html", bookings=result["items"], pagination=result, activity=activity
    )


@app.route("/bookings/<int:booking_id>/status", methods=["POST"])
@login_required
def booking_update_status(booking_id):
    new_status = (request.form.get("status") or "").strip()
    if new_status not in BOOKING_STATUSES:
        flash("Недопустимый статус заявки")
        return redirect(url_for("bookings_list"))
    # Один запрос вместо двух (get_booking_by_id + update_booking_status) —
    # old_status получаем прямо из UPDATE, см. update_booking_status_and_get.
    updated = db.update_booking_status_and_get(booking_id, new_status)
    if not updated:
        flash("Заявка не найдена")
        return redirect(url_for("bookings_list"))
    old_status = updated["old_status"]

    # Если заявку подтвердили и оплаты для неё ещё нет — создаём запись об
    # ожидаемой оплате. Проверка на существование обязательна: с тех пор,
    # как бот сам предлагает оплату сразу после записи (см.
    # process_booking_confirm в bot.py), платёж почти всегда уже существует
    # к этому моменту — без проверки здесь создавался бы дублирующий
    # payment на ту же заявку. get_payment_check_for_booking отдаёт
    # payment_id/цену/slots_count одним запросом вместо трёх.
    if new_status == "подтверждена":
        check = db.get_payment_check_for_booking(booking_id)
        if check and not check["payment_id"]:
            amount = float(check["game_price"]) * (check.get("slots_count") or 1)
            db.create_payment(booking_id, amount)

    # Смена статуса (особенно на/с "отменена") меняет число занятых мест,
    # которое бот показывает рядом с игрой — сбрасываем тот же кэш.
    cache.invalidate_games_cache()
    user_name = updated.get("user_name") or "—"
    if new_status == "отменена":
        # Отмена до оплаты — убираем «висящий» платёж из раздела Оплаты.
        db.delete_pending_payments_for_booking(booking_id)
        description = f"Бронирование №{booking_id} отменено (игрок: {user_name})"
    else:
        description = (
            f"Статус бронирования №{booking_id} изменён с «{old_status}» "
            f"на «{new_status}» (игрок: {user_name})"
        )
    log_admin_action("update_status", "booking", booking_id, description=description, old=old_status, new=new_status)

    flash("Статус заявки обновлён")
    return redirect(url_for("bookings_list"))


# ---------------------------------------------------------------------------
# Оплаты
# ---------------------------------------------------------------------------

@app.route("/payments")
@login_required
def payments_list():
    search = request.args.get("search", "")
    status_filter = request.args.get("status", "")
    page = request.args.get("page", 1, type=int)
    result = db.get_payments_paginated(
        search=search, status=status_filter, page=page, per_page=DEFAULT_PAGE_SIZE
    )
    activity = db.get_latest_activity_marker()
    # Открыли раздел — бейдж "+N" у "Оплаты" в шапке пропадает. Отмечаем
    # именно last_payment_notified_at (а не max_payment_id) — см.
    # count_new_since в database.py.
    _mark_section_seen(payment_notified_at=activity["last_payment_notified_at"])
    return render_template(
        "payments.html", payments=result["items"], pagination=result, activity=activity
    )


@app.route("/payments/<int:payment_id>/confirm", methods=["POST"])
@login_required
def payment_confirm(payment_id):
    # Контекст для уведомления игрока получаем ДО confirm_payment — если
    # что-то пойдёт не так с отправкой сообщения, это не должно повлиять
    # на сам факт подтверждения оплаты в базе.
    context = db.get_payment_notification_context(payment_id)
    if not context:
        flash("Платёж не найден")
        return redirect(url_for("payments_list"))
    if context["status"] != "ожидает":
        # Платёж уже подтверждён или по нему оформлен возврат (например,
        # игрок отменил бронь более чем за 12ч до игры) — повторное
        # подтверждение не имеет смысла и могло бы затереть статус «возврат».
        flash(f"Платёж уже в статусе «{context['status']}» — подтверждение не требуется")
        return redirect(url_for("payments_list"))

    db.confirm_payment(payment_id)
    if context:
        description = (
            f"Статус оплаты для брони №{context['booking_id']} изменён с «ожидает» "
            f"на «подтверждена» (игрок: {context['user_name']}, сумма: {_fmt_money(context['amount'])} руб.)"
        )
    else:
        description = f"Статус оплаты №{payment_id} изменён с «ожидает» на «подтверждена»"
    log_admin_action("confirm", "payment", payment_id, description=description, old="ожидает", new="подтверждена")

    if context and context.get("telegram_id"):
        game_dt = f"{context['game_date'].strftime('%d.%m.%Y')} в {str(context['game_time'])[:5]}"
        loc = html.escape(str(context.get("location") or ""), quote=False)
        text = (
            "✅ <b>Оплата подтверждена!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Сумма: {float(context['amount']):.0f} ₽\n"
            f"📅 {html.escape(game_dt, quote=False)}\n"
            f"📍 {loc}\n\n"
            "Ждём тебя на корте! 🎾"
        )
        send_telegram_message(context["telegram_id"], text)

    flash("Оплата подтверждена")
    return redirect(url_for("payments_list"))


@app.route("/payments/<int:payment_id>/confirm_refund", methods=["POST"])
@login_required
def payment_confirm_refund(payment_id):
    """Финальное подтверждение возврата: бронь была отменена игроком более
    чем за 12ч до игры (см. process_cancel_yes в bot.py), платёж автоматически
    получил статус 'возврат' — но реальный перевод денег администратор
    делает вручную вне системы, и эта кнопка фиксирует, что перевод сделан."""
    context = db.get_payment_notification_context(payment_id)
    if not context:
        flash("Платёж не найден")
        return redirect(url_for("payments_list"))
    if context["status"] != "возврат":
        flash(f"Платёж в статусе «{context['status']}» — оформление возврата не требуется")
        return redirect(url_for("payments_list"))

    updated = db.confirm_refund(payment_id)
    if not updated:
        flash("Не удалось оформить возврат — статус платежа уже изменился")
        return redirect(url_for("payments_list"))

    description = (
        f"Возврат оплаты для брони №{context['booking_id']} оформлен администратором "
        f"(игрок: {context['user_name']}, сумма: {_fmt_money(context['amount'])} руб.)"
    )
    log_admin_action(
        "confirm_refund", "payment", payment_id, description=description,
        old="возврат", new="возврат оформлен",
    )
    flash("Возврат оформлен")
    return redirect(url_for("payments_list"))


# ---------------------------------------------------------------------------
# Посещения
# ---------------------------------------------------------------------------

@app.route("/visits")
@login_required
def visits_list():
    page = request.args.get("page", 1, type=int)
    result = db.get_visits_paginated(page=page, per_page=DEFAULT_PAGE_SIZE)
    return render_template("visits.html", visits=result["items"], pagination=result)


@app.route("/visits/<int:booking_id>/mark", methods=["POST"])
@login_required
def visit_mark(booking_id):
    updated = db.mark_booking_visited_and_get(booking_id)
    if not updated:
        flash("Заявка не найдена")
        return redirect(url_for("visits_list"))
    description = f"Посещение отмечено для брони №{booking_id} (игрок: {updated.get('user_name') or '—'})"
    log_admin_action("mark_visited", "booking", booking_id, description=description, new="посещена")
    flash("Посещение отмечено")
    return redirect(url_for("visits_list"))


# ---------------------------------------------------------------------------
# Клубы
# ---------------------------------------------------------------------------

@app.route("/clubs")
@login_required
def clubs_list():
    page = request.args.get("page", 1, type=int)
    result = db.get_clubs_paginated(page=page, per_page=DEFAULT_PAGE_SIZE)
    return render_template("clubs.html", clubs=result["items"], pagination=result)


@app.route("/clubs/new", methods=["GET", "POST"])
@login_required
def club_new():
    if request.method == "POST":
        city = (request.form.get("city") or "").strip()
        if not city:
            flash("Укажите город клуба")
            return render_template("club_form.html", club=None)
        club = db.create_club(
            name=request.form["name"],
            city=city,
            address=request.form["address"],
            phone=request.form["phone"],
            description=request.form.get("description", ""),
        )
        description = (
            f"Клуб «{club['name']}» добавлен ({club.get('city') or '—'}, {club['address']})"
        )
        log_admin_action("create", "club", club["id"], description=description)
        flash("Клуб добавлен")
        return redirect(url_for("clubs_list"))
    return render_template("club_form.html", club=None)


@app.route("/clubs/<int:club_id>/edit", methods=["GET", "POST"])
@login_required
def club_edit(club_id):
    club = db.get_club_by_id(club_id)
    if not club:
        flash("Клуб не найден")
        return redirect(url_for("clubs_list"))

    if request.method == "POST":
        city = (request.form.get("city") or "").strip()
        if not city:
            flash("Укажите город клуба")
            return render_template("club_form.html", club=club)
        old_snapshot = dict(club)
        db.update_club(
            club_id=club_id,
            name=request.form["name"],
            city=city,
            address=request.form["address"],
            phone=request.form["phone"],
            description=request.form.get("description", ""),
        )
        updated = db.get_club_by_id(club_id)
        description = f"Клуб «{updated['name']}» отредактирован: {_describe_club_diff(old_snapshot, dict(updated))}"
        log_admin_action("update", "club", club_id, description=description)
        flash("Клуб обновлён")
        return redirect(url_for("clubs_list"))

    return render_template("club_form.html", club=club)


# ---------------------------------------------------------------------------
# Журнал действий
# ---------------------------------------------------------------------------

@app.route("/logs")
@login_required
def logs_list():
    page = request.args.get("page", 1, type=int)
    entity_type = request.args.get("entity_type", "")
    result = db.get_logs_paginated(page=page, per_page=DEFAULT_PAGE_SIZE, entity_type=entity_type)
    entity_types = db.get_distinct_log_entity_types()
    return render_template(
        "logs.html", logs=result["items"], pagination=result,
        entity_types=entity_types, selected_entity_type=entity_type,
    )


# ---------------------------------------------------------------------------
# О клубе
# ---------------------------------------------------------------------------

@app.route("/about")
@login_required
def about_club():
    club_info = db.get_club_info()
    return render_template("about_club.html", club_info=club_info)


@app.route("/about/update", methods=["POST"])
@login_required
def about_club_update():
    old_info = db.get_club_info()
    raw_admin_id = (request.form.get("admin_telegram_id") or "").strip()
    admin_telegram_id = raw_admin_id  # '' очищает; число — сохраняет
    if raw_admin_id and not raw_admin_id.isdigit():
        flash("Telegram ID администратора должен быть числом")
        return redirect(url_for("about_club"))
    raw_admin_user = (request.form.get("admin_telegram_username") or "").strip().lstrip("@")
    db.update_club_info(
        name=request.form["name"],
        description=request.form["description"],
        contact_phone=request.form["contact_phone"],
        contact_email=request.form.get("contact_email", ""),
        admin_telegram_id=admin_telegram_id,
        admin_telegram_username=raw_admin_user,
    )
    new_info = db.get_club_info()
    description = f"Информация о клубе обновлена: {_describe_club_info_diff(dict(old_info) if old_info else {}, dict(new_info) if new_info else {})}"
    log_admin_action(
        "update", "club_info",
        old_info["id"] if old_info else None,
        description=description,
    )
    flash("Информация о клубе обновлена")
    return redirect(url_for("about_club"))


# ---------------------------------------------------------------------------
# Отзывы
# ---------------------------------------------------------------------------

@app.route("/reviews")
@login_required
def reviews_list():
    reviews = db.get_all_reviews()
    # Открыли раздел — бейдж "+N" у "Отзывы" в шапке пропадает.
    activity = db.get_latest_activity_marker()
    _mark_section_seen(review_id=activity["max_review_id"])
    return render_template("reviews.html", reviews=reviews, activity=activity)


# ---------------------------------------------------------------------------
# Отчёт в Excel
# ---------------------------------------------------------------------------

@app.route("/report/excel")
@login_required
def report_excel():
    # Один агрегирующий запрос вместо 1 + 2N (раньше на каждую игру уходило
    # по два дополнительных запроса — count_bookings + sum(payments)).
    games = db.get_all_games_with_stats()

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчёт по играм"

    headers = ["Дата", "Время", "Место", "Цена", "Мест всего", "Записалось", "Собрано оплат"]
    ws.append(headers)

    for g in games:
        ws.append([
            g["game_date"].strftime("%d.%m.%Y"),
            str(g["game_time"])[:5],
            g["location"],
            float(g["price"]),
            g["total_slots"],
            g["taken"],
            float(g["collected"]),
        ])

    # Немного расширяем колонки, чтобы текст помещался
    for col_idx, header in enumerate(headers, start=1):
        ws.column_dimensions[chr(64 + col_idx)].width = max(15, len(header) + 5)

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    filename = f"padel_report_{datetime.now(APP_TIMEZONE).strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------------------------------------------------------------------
# Webhook для Telegram-бота
# ---------------------------------------------------------------------------

@app.route(WEBHOOK_PATH, methods=["POST"])
@csrf.exempt  # Telegram отправляет JSON без CSRF-токена — это не браузерная форма
@limiter.limit(os.getenv("WEBHOOK_RATE_LIMIT", "30 per second"))
def webhook():
    """Принимает обновления от Telegram через webhook.

    Secret-token опционален (WEBHOOK_ENFORCE_SECRET=1): на Render из-за
    рассинхрона FLASK_SECRET_KEY / WEBHOOK_SECRET_TOKEN Telegram слал
    апдейты, а мы отвечали 403 — бот «молчал», хотя webhook был
    зарегистрирован. По умолчанию принимаем HTTPS POST на секретный путь.

    Обработка в event loop бота через run_coroutine_threadsafe; Telegram
    достаточно ответа 200 OK сразу."""
    enforce_secret = os.getenv("WEBHOOK_ENFORCE_SECRET", "0").lower() in {"1", "true", "yes"}
    if enforce_secret:
        header_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "") or ""
        expected = WEBHOOK_SECRET_TOKEN or ""
        # compare_digest на разных длинах ведёт себя по-разному в версиях
        # Python — сначала явная проверка длины.
        if (
            not expected
            or len(header_token) != len(expected)
            or not secrets.compare_digest(header_token, expected)
        ):
            logger.warning(
                "Отклонён webhook без/с неверным secret token (IP %s)",
                get_remote_address(),
            )
            return {"status": "forbidden"}, 403

    if bot_instance and dp_instance and bot_loop:
        try:
            update = Update.model_validate(request.json)
        except Exception as e:
            logger.error("Ошибка разбора webhook-обновления: %s", e)
            return {"status": "error"}, 400

        future = asyncio.run_coroutine_threadsafe(
            dp_instance.feed_webhook_update(bot_instance, update), bot_loop
        )

        def _log_webhook_failure(fut):
            exc = fut.exception() if fut.done() else None
            if exc:
                logger.error("Ошибка фоновой обработки webhook-обновления: %s", exc)

        future.add_done_callback(_log_webhook_failure)
        return {"status": "ok"}
    return {"status": "bot not ready"}, 503


def run_bot():
    """Запускает Telegram-бота в отдельном потоке с собственным event loop."""
    global bot_instance, dp_instance, bot_loop, bot_start_error

    from apscheduler.schedulers.background import BackgroundScheduler
    from bot import router, send_reminders, setup_bot_commands
    import database_async as db_async

    bot_start_error = None
    WEBHOOK_URL = (os.getenv("WEBHOOK_URL") or "").rstrip("/")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bot_loop = loop

    if not BOT_TOKEN:
        bot_start_error = "BOT_TOKEN не задан"
        raise RuntimeError(bot_start_error)

    bot_instance = Bot(token=BOT_TOKEN)
    dp_instance = Dispatcher(storage=MemoryStorage())
    dp_instance.include_router(router)

    scheduler = BackgroundScheduler()

    def _reminders_job():
        future = asyncio.run_coroutine_threadsafe(send_reminders(bot_instance), loop)
        try:
            future.result(timeout=120)
        except Exception as e:
            logger.error("Ошибка задачи напоминаний: %s", e)

    async def _startup():
        await db_async.get_pool()
        asyncio.create_task(db_async.keepalive_loop())
        await setup_bot_commands(bot_instance)

        scheduler.add_job(_reminders_job, "interval", minutes=15)
        scheduler.start()

        if WEBHOOK_URL:
            webhook_endpoint = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
            enforce_secret = os.getenv("WEBHOOK_ENFORCE_SECRET", "0").lower() in {"1", "true", "yes"}
            set_kwargs = {
                "url": webhook_endpoint,
                "drop_pending_updates": False,
                "allowed_updates": ["message", "callback_query", "pre_checkout_query"],
            }
            if enforce_secret and WEBHOOK_SECRET_TOKEN:
                set_kwargs["secret_token"] = WEBHOOK_SECRET_TOKEN
            await bot_instance.set_webhook(**set_kwargs)
            logger.warning(
                "Telegram webhook зарегистрирован: %s (secret=%s)",
                webhook_endpoint,
                "on" if set_kwargs.get("secret_token") else "off",
            )
        else:
            logger.warning("WEBHOOK_URL пуст — long polling")
            await bot_instance.delete_webhook(drop_pending_updates=False)
            await dp_instance.start_polling(bot_instance)

    try:
        loop.run_until_complete(_startup())
        if WEBHOOK_URL:
            logger.warning("Bot event loop run_forever()")
            loop.run_forever()
    except Exception as e:
        bot_start_error = f"{type(e).__name__}: {e}"
        logger.exception("Ошибка запуска бота: %s", e)
    finally:
        scheduler.shutdown(wait=False)
        try:
            if not loop.is_closed():
                loop.run_until_complete(db_async.close_pool())
        except Exception:
            pass
        try:
            if not loop.is_closed():
                loop.close()
        except Exception:
            pass


_infra_started = False
_infra_lock = threading.Lock()
_cleanup_scheduler = None


def start_infra_services():
    """Keepalive БД + ежедневная очистка. Не блокирует HTTP. Idempotent."""
    global _infra_started, _cleanup_scheduler
    with _infra_lock:
        if _infra_started:
            return
        _infra_started = True
    try:
        db.migrate_db()
    except Exception:
        logger.exception("migrate_db при старте не удалась")
    try:
        db.start_keepalive_thread()
    except Exception:
        logger.exception("Не удалось запустить DB keepalive")
    try:
        _cleanup_scheduler = start_cleanup_scheduler()
    except Exception:
        logger.exception("Не удалось запустить cleanup scheduler")


def start_background_services():
    """Запускает бота в worker-процессе. Idempotent (gunicorn post_worker_init)."""
    global _bot_services_started, bot_start_error

    start_infra_services()

    if os.getenv("RUN_BOT_IN_BACKGROUND", "0").lower() in {"0", "false", "no"}:
        bot_start_error = "RUN_BOT_IN_BACKGROUND выключен"
        logger.warning("Бот не стартует: %s", bot_start_error)
        return

    with _bot_services_lock:
        if _bot_services_started:
            return
        _bot_services_started = True

    def _start():
        delay = float(os.getenv("BOT_START_DELAY_SECONDS", "0"))
        if delay > 0:
            logger.warning("Бот: ждём %.0fс перед стартом (чтобы /health был жив)", delay)
            time.sleep(delay)
        try:
            run_bot()
        except Exception as e:
            global bot_start_error
            bot_start_error = f"{type(e).__name__}: {e}"
            logger.exception("Фоновый поток бота завершился с ошибкой")

    threading.Thread(target=_start, daemon=True, name="padel-bot").start()
    logger.warning("Бот: фоновый поток запланирован")


def _run_cleanup_job():
    """Ежедневная очистка старых данных:
    - bookings старше DATA_RETENTION_DAYS дней по дате самой ИГРЫ
      (games.game_date) — брони удаляются только для игр, прошедших больше
      этого срока назад, поэтому у затронутых игр физически не может быть
      "будущих" броней, а сами игры (таблица games) эта задача не трогает
      вообще, даже если у них не осталось ни одной брони.
    - admin_logs старше DATA_RETENTION_DAYS дней по created_at.

    Итоговая запись о количестве удалённых строк пишется в admin_logs
    ПОСЛЕ обеих очисток — она создаётся уже позже точки отсчёта "старше N
    дней", поэтому не попадёт под то же (или следующее) удаление логов."""
    try:
        deleted_bookings = db.delete_old_bookings(days=DATA_RETENTION_DAYS)
        deleted_logs = db.delete_old_admin_logs(days=DATA_RETENTION_DAYS)
        description = (
            f"Автоочистка данных старше {DATA_RETENTION_DAYS} дн.: "
            f"удалено бронирований — {deleted_bookings}, "
            f"удалено записей журнала — {deleted_logs}."
        )
        log_admin_action("cleanup", "system", None, description=description)
        logger.info(description)
    except Exception as e:
        logger.error("Ошибка ежедневной очистки старых данных: %s", e)


def start_cleanup_scheduler():
    """Планировщик ежедневной очистки — запускается всегда вместе с
    процессом CRM (app.py), независимо от RUN_BOT_IN_BACKGROUND (в отличие
    от напоминаний бота, которым нужен встроенный бот в этом же процессе).
    Время (03:00) считается в APP_TIMEZONE, а не в UTC — иначе задача
    срабатывала бы не ночью, а в 6 утра по московскому времени.

    ВАЖНО про несколько gunicorn worker-процессов: как и с ботом (см.
    start_background_services()), при --workers>1 каждый worker поднимет
    свой планировщик и попытается выполнить очистку отдельно в 03:00.
    Сами DELETE-запросы идемпотентны (повторное удаление уже удалённых
    строк безвредно и просто отработает на 0 строк), но в журнале появится
    по одной записи "Автоочистка..." от каждого воркера. Чтобы не плодить
    дублирующиеся записи в журнале — используйте --workers 1 (см.
    deploy.md/docker-compose.yml), как и рекомендовано для бота."""
    from apscheduler.schedulers.background import BackgroundScheduler

    scheduler = BackgroundScheduler(timezone=APP_TIMEZONE)
    scheduler.add_job(_run_cleanup_job, "cron", hour=3, minute=0, id="daily_cleanup")
    scheduler.start()
    logger.info("Планировщик ежедневной очистки данных запущен (03:00 %s)", APP_TIMEZONE)
    return scheduler


# Keepalive/cleanup/бот НЕ стартуем при import — иначе под gunicorn worker
# может зависнуть до bind, а Render healthCheckPath=/health даст белый экран.
# Старт: gunicorn.conf.py post_worker_init → start_infra/background_services,
# либо python app.py ниже.
if os.getenv("GUNICORN_PID") is None and not _IS_RENDER and __name__ != "__main__":
    # Импорт модуля локально без gunicorn (редко) — поднимем infra без бота.
    start_infra_services()

if __name__ == '__main__':
    # Локально по умолчанию только localhost; на PaaS (Render и т.п.) задайте
    # HOST=0.0.0.0. Debug — только через FLASK_DEBUG=1.
    start_background_services()
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", 5000))
    app.run(host=host, port=port, debug=app.debug)
