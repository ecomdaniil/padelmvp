"""
init_db.py
----------
Запустите этот файл ОДИН РАЗ, чтобы создать все нужные таблицы в базе данных.

Команда для запуска (когда виртуальное окружение активировано):
    python init_db.py
"""

from database import init_db, migrate_db

if __name__ == "__main__":
    print("Подключаюсь к базе данных и создаю таблицы...")
    init_db()
    print("Применяю миграции (новые поля users, индексы для games/bookings/payments/logs)...")
    migrate_db()
    print(
        "Готово! Таблицы users, games, bookings, payments, reviews, clubs, "
        "admin_logs, club_info и индексы созданы (или уже существовали)."
    )
