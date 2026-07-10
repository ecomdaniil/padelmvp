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
import os
from datetime import datetime
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, send_file
)
from openpyxl import Workbook

import database as db

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

ADMIN_LOGIN = os.getenv("ADMIN_LOGIN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")


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
def login():
    if request.method == "POST":
        login_value = request.form.get("login", "")
        password_value = request.form.get("password", "")

        # Сравниваем с данными из .env. Никаких паролей в коде!
        if login_value == ADMIN_LOGIN and password_value == ADMIN_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("games_list"))
        else:
            flash("Неверный логин или пароль")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    return redirect(url_for("games_list"))


# ---------------------------------------------------------------------------
# Игры
# ---------------------------------------------------------------------------

@app.route("/games")
@login_required
def games_list():
    games = db.get_all_games()
    games_with_stats = []
    for g in games:
        taken = db.count_bookings_for_game(g["id"])
        collected = db.get_confirmed_payments_sum_for_game(g["id"])
        games_with_stats.append({**g, "taken": taken, "collected": collected})
    return render_template("games.html", games=games_with_stats)


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
        flash("Игра обновлена")
        return redirect(url_for("games_list"))

    return render_template("game_form.html", game=game)


# ---------------------------------------------------------------------------
# Заявки (bookings)
# ---------------------------------------------------------------------------

@app.route("/bookings")
@login_required
def bookings_list():
    bookings = db.get_all_bookings()
    return render_template("bookings.html", bookings=bookings)


@app.route("/bookings/<int:booking_id>/status", methods=["POST"])
@login_required
def booking_update_status(booking_id):
    new_status = request.form["status"]
    db.update_booking_status(booking_id, new_status)

    # Если заявку подтвердили — сразу создаём запись об ожидаемой оплате
    if new_status == "подтверждена":
        booking = db.get_booking_by_id(booking_id)
        game = db.get_game_by_id(booking["game_id"])
        db.create_payment(booking_id, game["price"])

    flash("Статус заявки обновлён")
    return redirect(url_for("bookings_list"))


# ---------------------------------------------------------------------------
# Оплаты
# ---------------------------------------------------------------------------

@app.route("/payments")
@login_required
def payments_list():
    payments = db.get_all_payments()
    return render_template("payments.html", payments=payments)


@app.route("/payments/<int:payment_id>/confirm", methods=["POST"])
@login_required
def payment_confirm(payment_id):
    db.confirm_payment(payment_id)
    flash("Оплата подтверждена")
    return redirect(url_for("payments_list"))


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
    games = db.get_all_games()

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчёт по играм"

    headers = ["Дата", "Время", "Место", "Цена", "Мест всего", "Записалось", "Собрано оплат"]
    ws.append(headers)

    for g in games:
        taken = db.count_bookings_for_game(g["id"])
        collected = db.get_confirmed_payments_sum_for_game(g["id"])
        ws.append([
            g["game_date"].strftime("%d.%m.%Y"),
            str(g["game_time"])[:5],
            g["location"],
            float(g["price"]),
            g["total_slots"],
            taken,
            float(collected),
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


if __name__ == "__main__":
    app.run(debug=True, port=5000)
