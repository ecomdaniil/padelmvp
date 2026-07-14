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
import logging
import os
import secrets
from datetime import datetime
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, send_file
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

DEFAULT_PAGE_SIZE = int(os.getenv("CRM_PAGE_SIZE", "20"))

# Глобальные переменные для бота: единственные Bot/Dispatcher на процесс
# (раньше bot.py создавал свою отдельную пару внутри main(), что приводило
# к путанице — теперь webhook всегда обслуживается вот этими инстансами).
bot_instance = None
dp_instance = None
bot_loop = None


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


@app.route("/")
@login_required
def index():
    # Раньше здесь читались все строки каждой таблицы только чтобы посчитать
    # len(...) — теперь используются лёгкие COUNT(*)-запросы.
    summary = {
        "games": db.count_games(),
        "bookings": db.count_active_bookings(),
        "pending_payments": db.count_pending_payments(),
        "visits": db.count_visits(),
        "clubs": db.count_clubs(),
        "logs": db.count_logs(),
    }
    return render_template("dashboard.html", summary=summary)


@app.route("/health")
def health_check():
    return {"status": "ok"}, 200


# ---------------------------------------------------------------------------
# Игры
# ---------------------------------------------------------------------------

@app.route("/games")
@login_required
def games_list():
    page = request.args.get("page", 1, type=int)
    result = db.get_games_paginated(page=page, per_page=DEFAULT_PAGE_SIZE)
    return render_template("games.html", games=result["items"], pagination=result)


@app.route("/games/new", methods=["GET", "POST"])
@login_required
def game_new():
    if request.method == "POST":
        db.create_game(
            game_date=request.form["game_date"],
            game_time=request.form["game_time"],
            location=request.form["location"],
            price=request.form["price"],
            total_slots=request.form["total_slots"],
        )
        # Бот кэширует список ближайших игр (см. cache.py/bot.py) — без
        # явного сброса здесь новая игра была видна в боте только после
        # истечения TTL кэша или перезапуска процесса бота.
        cache.invalidate_games_cache()
        flash("Игра создана")
        return redirect(url_for("games_list"))
    return render_template("game_form.html", game=None)


@app.route("/games/<int:game_id>/edit", methods=["GET", "POST"])
@login_required
def game_edit(game_id):
    game = db.get_game_by_id(game_id)
    if not game:
        flash("Игра не найдена")
        return redirect(url_for("games_list"))

    if request.method == "POST":
        db.update_game(
            game_id=game_id,
            game_date=request.form["game_date"],
            game_time=request.form["game_time"],
            location=request.form["location"],
            price=request.form["price"],
            total_slots=request.form["total_slots"],
        )
        cache.invalidate_games_cache()
        flash("Игра обновлена")
        return redirect(url_for("games_list"))

    return render_template("game_form.html", game=game)


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
    return render_template("bookings.html", bookings=result["items"], pagination=result)


@app.route("/bookings/<int:booking_id>/status", methods=["POST"])
@login_required
def booking_update_status(booking_id):
    new_status = request.form["status"]
    db.update_booking_status(booking_id, new_status)

    # Если заявку подтвердили и оплаты для неё ещё нет — создаём запись об
    # ожидаемой оплате. Проверка на существование обязательна: с тех пор,
    # как бот сам предлагает оплату сразу после записи (см.
    # process_booking_confirm в bot.py), платёж почти всегда уже существует
    # к этому моменту — без проверки здесь создавался бы дублирующий
    # payment на ту же заявку.
    if new_status == "подтверждена" and not db.get_payment_for_booking(booking_id):
        booking = db.get_booking_by_id(booking_id)
        game = db.get_game_by_id(booking["game_id"])
        amount = float(game["price"]) * booking.get("slots_count", 1)
        db.create_payment(booking_id, amount)

    # Смена статуса (особенно на/с "отменена") меняет число занятых мест,
    # которое бот показывает рядом с игрой — сбрасываем тот же кэш.
    cache.invalidate_games_cache()

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
    return render_template("payments.html", payments=result["items"], pagination=result)


@app.route("/payments/<int:payment_id>/confirm", methods=["POST"])
@login_required
def payment_confirm(payment_id):
    db.confirm_payment(payment_id)
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
    db.mark_booking_visited(booking_id)
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
        db.create_club(
            name=request.form["name"],
            address=request.form["address"],
            phone=request.form["phone"],
            description=request.form.get("description", ""),
        )
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
        db.update_club(
            club_id=club_id,
            name=request.form["name"],
            address=request.form["address"],
            phone=request.form["phone"],
            description=request.form.get("description", ""),
        )
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
    result = db.get_logs_paginated(page=page, per_page=DEFAULT_PAGE_SIZE)
    return render_template("logs.html", logs=result["items"], pagination=result)


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
    db.update_club_info(
        name=request.form["name"],
        description=request.form["description"],
        contact_phone=request.form["contact_phone"],
        contact_email=request.form.get("contact_email", ""),
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
    созданный в другом loop."""
    if bot_instance and dp_instance and bot_loop:
        try:
            update = Update.model_validate(request.json)
            future = asyncio.run_coroutine_threadsafe(
                dp_instance.feed_webhook_update(bot_instance, update), bot_loop
            )
            future.result(timeout=10)
            return {"status": "ok"}
        except Exception as e:
            logger.error("Ошибка обработки webhook: %s", e)
            return {"status": "error"}, 500
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


start_background_services()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
