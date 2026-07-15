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

import io
import json
import logging
import os
import secrets
import urllib.request
from datetime import date, datetime, time as time_type
from decimal import Decimal
from functools import wraps

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

import cache
import database as db

import threading
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from aiogram.fsm.storage.memory import MemoryStorage

load_dotenv()

# По умолчанию в логах остаются только ошибки — уровень можно поднять через
# .env (LOG_LEVEL=INFO/DEBUG), например для отладки на staging.
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.ERROR))
logger = logging.getLogger(__name__)

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
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=[f"{os.getenv('CRM_RATE_LIMIT_PER_SECOND', '10')} per second"],
    storage_uri=os.getenv("REDIS_URL", "memory://"),
)

ADMIN_LOGIN = os.getenv("ADMIN_LOGIN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
if not ADMIN_LOGIN or not ADMIN_PASSWORD:
    raise RuntimeError(
        "Не найдены ADMIN_LOGIN/ADMIN_PASSWORD в .env. "
        "Задайте логин и надёжный пароль перед запуском CRM."
    )

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")


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
}
ENTITY_LABELS = {
    "game": "Игра",
    "booking": "Бронирование",
    "payment": "Оплата",
    "club": "Клуб",
    "club_info": "Информация о клубе",
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


@app.route("/login", methods=["GET", "POST"])
@limiter.limit(os.getenv("LOGIN_RATE_LIMIT", "5 per minute"))
def login():
    if request.method == "POST":
        login_value = request.form.get("login", "")
        password_value = request.form.get("password", "")

        # Сравниваем с данными из .env. Никаких паролей в коде!
        # secrets.compare_digest — защита от timing-атак на сравнение строк.
        valid_login = bool(ADMIN_LOGIN) and secrets.compare_digest(login_value, ADMIN_LOGIN)
        valid_password = bool(ADMIN_PASSWORD) and secrets.compare_digest(password_value, ADMIN_PASSWORD)

        if valid_login and valid_password:
            session.clear()
            session["logged_in"] = True
            return redirect(url_for("games_list"))
        else:
            flash("Неверный логин или пароль")

    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


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
    try:
        return render_template("error.html", message=str(err) if app.debug else None), 500
    except Exception:
        return "Произошла ошибка на сервере. Попробуйте обновить страницу.", 500


@app.route("/")
@login_required
def index():
    # Один запрос с 6 подзапросами вместо 6 отдельных round-trip'ов к БД
    # (см. db.get_dashboard_summary) — на управляемом Postgres каждый лишний
    # round-trip добавлял заметную задержку к открытию главной страницы.
    summary = db.get_dashboard_summary()
    return render_template("dashboard.html", summary=summary)


@app.route("/health")
def health_check():
    return {"status": "ok"}, 200


@app.route("/api/activity")
@login_required
def api_activity():
    """Лёгкий эндпоинт для поллинга с bookings.html/payments.html — фронтенд
    раз в несколько секунд сверяет счётчики с тем, что было при загрузке
    страницы, и если что-то новое появилось (например, бронирование и
    оплата из бота), показывает баннер «Обновить». Одна простая агрегатная
    выборка (см. db.get_latest_activity_marker), не создаёт заметной
    нагрузки даже при частом опросе."""
    return db.get_latest_activity_marker(), 200


def _json_value(value):
    """psycopg2 отдаёт Decimal/date/time/datetime, которые стандартный JSON
    не умеет сериализовать сам — приводим к простым JSON-совместимым типам."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
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


def _validate_game_values(values, clubs_by_id):
    """Валидирует все поля формы игры. Возвращает (errors, parsed) — при
    непустом errors значения в parsed могут быть неполными/некорректными,
    использовать их для сохранения в БД нельзя."""
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

    if not values["city"]:
        errors.append("Укажите город.")
    parsed["city"] = values["city"][:100]

    parsed["club_id"] = None
    if values["club_id"]:
        try:
            club_id_int = int(values["club_id"])
        except ValueError:
            errors.append("Некорректно выбран клуб.")
        else:
            if club_id_int not in clubs_by_id:
                errors.append("Выбранный клуб не найден.")
            else:
                parsed["club_id"] = club_id_int

    if not values["address"]:
        errors.append("Укажите адрес.")
    parsed["address"] = values["address"][:255]

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
            errors.append("Занято мест не может быть отрицательным.")
        elif "total_slots" in parsed and booked_places > parsed["total_slots"]:
            errors.append("Занято мест не может превышать максимальное количество игроков.")
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

    result = db.get_games_paginated(
        page=page, per_page=DEFAULT_PAGE_SIZE, level=level, city=city,
        date_from=_safe_date(date_from_raw), date_to=_safe_date(date_to_raw),
        time_from=_safe_time(time_from_raw), time_to=_safe_time(time_to_raw),
    )
    cities = db.get_distinct_game_cities()
    return render_template(
        "games.html", games=result["items"], pagination=result,
        levels=GAME_LEVELS, selected_level=level,
        cities=cities, selected_city=city,
        date_from=date_from_raw, date_to=date_to_raw,
        time_from=time_from_raw, time_to=time_to_raw,
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
        errors, parsed = _validate_game_values(values, clubs_by_id)
        if errors:
            for e in errors:
                flash(e)
            return render_template(
                "game_form.html", game=game, values=values, clubs=clubs, levels=GAME_LEVELS,
                actual_taken=db.count_bookings_for_game(game_id),
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

        # Ручное booked_places — независимое поле, не связанное со списком
        # реальных броней (bookings). Если админ поставил значение меньше
        # фактического числа занятых по бронированиям мест — это подозрительно
        # (может скрыть переполненность игры), но по требованию это не должно
        # блокировать сохранение — только фиксируется в логах для отладки.
        actual_taken = db.count_bookings_for_game(game_id)
        if parsed["booked_places"] < actual_taken:
            logger.warning(
                "Игра №%s: вручную установлено занято мест (%s) меньше фактического "
                "числа мест по активным бронированиям (%s)",
                game_id, parsed["booked_places"], actual_taken,
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
    return render_template(
        "bookings.html", bookings=result["items"], pagination=result, activity=activity
    )


@app.route("/bookings/<int:booking_id>/status", methods=["POST"])
@login_required
def booking_update_status(booking_id):
    new_status = request.form["status"]
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
        text = (
            "✅ <b>Оплата подтверждена!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Сумма: {float(context['amount']):.0f} ₽\n"
            f"📅 {game_dt}\n"
            f"📍 {context['location']}\n\n"
            "Ждём тебя на корте! 🎾"
        )
        send_telegram_message(context["telegram_id"], text)

    flash("Оплата подтверждена")
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
        club = db.create_club(
            name=request.form["name"],
            address=request.form["address"],
            phone=request.form["phone"],
            description=request.form.get("description", ""),
        )
        description = f"Клуб «{club['name']}» добавлен ({club['address']})"
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
        old_snapshot = dict(club)
        db.update_club(
            club_id=club_id,
            name=request.form["name"],
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
    db.update_club_info(
        name=request.form["name"],
        description=request.form["description"],
        contact_phone=request.form["contact_phone"],
        contact_email=request.form.get("contact_email", ""),
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
    return render_template("reviews.html", reviews=reviews)


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

    filename = f"padel_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
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
@limiter.exempt  # у Telegram свой троттлинг обновлений, наш общий лимит здесь не нужен
def webhook():
    """Принимает обновления от Telegram через webhook.

    Обработка передаётся в event loop бот-потока через
    run_coroutine_threadsafe, а не через asyncio.run() — иначе каждый запрос
    создавал бы новый event loop, в котором нельзя использовать asyncpg-пул,
    созданный в другом loop.

    ВАЖНО: раньше здесь стоял future.result(timeout=10) — запрос ждал ДО 10
    секунд обработки в другом потоке, прежде чем ответить. При
    `gunicorn --workers 1` (см. deploy.md/docker-compose.yml) это означает,
    что ЕДИНСТВЕННЫЙ обработчик запросов был занят все эти секунды и не мог
    ответить ни на один запрос к CRM — при активном использовании бота
    (несколько параллельных бронирований, оплата с генерацией QR и т.п.)
    админ-панель выглядела зависшей/упавшей, а при превышении таймаута
    gunicorn ("--timeout", по умолчанию 30с) воркер вообще убивался и
    перезапускался. Telegram не ждёт результата обработки — ему достаточно
    ответа 200 OK, поэтому теперь мы отвечаем СРАЗУ, а обработку отправляем
    в фон (ошибки не теряются — их ловит done-callback ниже)."""
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
    """Запускает Telegram-бота в отдельном потоке с собственным event loop.

    Loop живёт всё время работы процесса (run_forever в режиме webhook), это
    позволяет Flask-обработчику webhook() безопасно передавать в него корутины
    через run_coroutine_threadsafe и использовать один и тот же asyncpg-пул."""
    global bot_instance, dp_instance, bot_loop

    from apscheduler.schedulers.background import BackgroundScheduler
    from bot import router, send_reminders, setup_bot_commands
    import database_async as db_async

    WEBHOOK_URL = os.getenv("WEBHOOK_URL")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bot_loop = loop

    bot_instance = Bot(token=BOT_TOKEN)
    dp_instance = Dispatcher(storage=MemoryStorage())
    dp_instance.include_router(router)

    scheduler = BackgroundScheduler()

    def _reminders_job():
        # BackgroundScheduler работает в СВОЁМ отдельном потоке — не может
        # напрямую вызвать async send_reminders(), поэтому передаём корутину
        # в event loop бота (см. bot.py:_make_reminder_job — тот же приём).
        future = asyncio.run_coroutine_threadsafe(send_reminders(bot_instance), loop)
        try:
            future.result(timeout=60)
        except Exception as e:
            logger.error("Ошибка задачи напоминаний: %s", e)

    async def _startup():
        await db_async.get_pool()
        # Держит БД "тёплой", чтобы /start и первое сообщение после паузы
        # не ждали холодный старт Neon (~5с) — см. database_async.py:keepalive_loop.
        asyncio.create_task(db_async.keepalive_loop())
        await setup_bot_commands(bot_instance)

        # Интервал 15 минут — чтобы надёжно попадать в более узкое окно
        # 2-часового напоминания (1ч45м-2ч15м), см. send_reminders в bot.py.
        scheduler.add_job(_reminders_job, "interval", minutes=15)
        scheduler.start()

        if WEBHOOK_URL:
            await bot_instance.set_webhook(url=f"{WEBHOOK_URL}{WEBHOOK_PATH}")
        else:
            await dp_instance.start_polling(bot_instance)

    try:
        loop.run_until_complete(_startup())
        if WEBHOOK_URL:
            # В режиме long polling start_polling уже блокирует навсегда;
            # в режиме webhook держим loop живым для feed_webhook_update.
            loop.run_forever()
    except Exception as e:
        logger.error("Ошибка запуска бота: %s", e)
    finally:
        scheduler.shutdown(wait=False)
        try:
            loop.run_until_complete(db_async.close_pool())
        except Exception:
            pass
        loop.close()


def start_background_services():
    """Запускает бота в фоне только если явно разрешено в .env.

    Раньше здесь была проверка `if __name__ != "__main__": return`, из-за
    которой фоновый бот фактически НИКОГДА не запускался под gunicorn
    (gunicorn импортирует модуль как "app", а не выполняет его как скрипт,
    поэтому __name__ всегда отличен от "__main__") — то есть
    RUN_BOT_IN_BACKGROUND=1 из .env.example/deploy.md под gunicorn ничего
    не делал, и webhook отвечал {"status": "bot not ready"}.

    ВАЖНО про несколько gunicorn worker-процессов: каждый worker — это
    отдельный процесс со своей памятью, поэтому при --workers>1 в каждом из
    них поднимется собственный поток бота, свой APScheduler (напоминания
    будут слаться по разу от каждого воркера) и свой in-memory кэш (см.
    cache.py). Поэтому при RUN_BOT_IN_BACKGROUND=1 нужно либо запускать
    gunicorn с --workers 1 (см. docker-compose.yml), либо переносить бота в
    отдельный сервис/процесс (RUN_BOT_IN_BACKGROUND=0 + отдельный `python
    bot.py`), и в обоих случаях — задавать REDIS_URL, если процессов больше
    одного, чтобы кэш и rate-limit были общими."""
    if os.getenv("RUN_BOT_IN_BACKGROUND", "0").lower() in {"0", "false", "no"}:
        return

    bot_thread = threading.Thread(target=run_bot, daemon=True, name="padel-bot")
    bot_thread.start()
    logger.info("Бот запущен в фоновом режиме")


# Держит БД CRM "тёплой" независимо от того, запущен ли бот в этом же
# процессе — тот же смысл, что и database_async.keepalive_loop() для бота
# (см. её docstring): без этого первая страница CRM после долгого простоя
# ждала бы холодный старт Neon.
db.start_keepalive_thread()

start_background_services()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
