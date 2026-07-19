# Padel Club — бот записи и CRM

Система записи на игры и тренировки: Telegram-бот для игроков и веб-панель для администратора клуба. Общая база — PostgreSQL.

## Возможности

- регистрация игрока (имя, телефон, уровень);
- запись на игры и тренировки, отмена записи;
- уведомления администратору о новых заявках и оплатах;
- подтверждение оплат в CRM;
- напоминания перед событием, контроль недобора состава;
- площадки, тренеры, журнал действий, выгрузка отчёта в Excel.

## Состав проекта

| Файл / каталог | Назначение |
|---|---|
| `bot.py` | Telegram-бот (aiogram) |
| `bot_content.py` | тексты кнопок и разделов бота |
| `app.py` | CRM на Flask + webhook Telegram |
| `database.py` | синхронный доступ к БД (CRM) |
| `database_async.py` | async-доступ к БД (бот) |
| `payment_provider.py` | сценарий оплаты в боте |
| `cache.py` | кэш списка игр |
| `init_db.py` | создание схемы БД |
| `templates/` | страницы CRM |
| `.env.example` | образец настроек |

## Требования

- Python 3.11+
- PostgreSQL (локально, Neon или другой хостинг)
- токен бота от [@BotFather](https://t.me/BotFather)
- для продакшена — HTTPS-домен (webhook Telegram)

## Быстрый старт (локально)

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Заполните `.env` (минимум):

```env
BOT_TOKEN=...
DATABASE_URL=postgresql://...
ADMIN_LOGIN=admin
ADMIN_PASSWORD_HASH=...          # см. ниже
FLASK_SECRET_KEY=...             # python -c "import secrets; print(secrets.token_hex(32))"
ADMIN_CHAT_ID=...                # ваш числовой Telegram ID
APP_TIMEZONE=Europe/Moscow
RUN_BOT_IN_BACKGROUND=1
```

Хеш пароля CRM:

```bash
python -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('ваш_пароль', method='pbkdf2:sha256'))"
```

Инициализация БД (один раз):

```bash
python init_db.py
```

Запуск CRM и бота одним процессом:

```bash
python app.py
```

- CRM: http://127.0.0.1:5000  
- бот: напишите `/start` в Telegram  

Альтернатива — два процесса: `RUN_BOT_IN_BACKGROUND=0`, затем отдельно `python bot.py` и `python app.py`.

## Настройка клуба после установки

1. Войдите в CRM.
2. Раздел **Клубы** — площадки (город, адрес).
3. Раздел **Тренеры** — карточки тренеров для бота.
4. Раздел **О клубе** — название, контакты, Telegram ID администратора для уведомлений.
5. **Игры** / **Тренировки** — расписание.
6. При заявке оплаты в боте («Я оплатил») подтвердите платёж в **Оплаты**.

Название клуба из «О клубе» отображается в шапке CRM. Текст кнопки «О клубе» в боте задаётся в `bot_content.py` (`PADEL_INFO_TEXT`).

## Переменные окружения

Полный список — в `.env.example`. Важно для продакшена:

| Переменная | Назначение |
|---|---|
| `BOT_TOKEN` | токен Telegram-бота |
| `DATABASE_URL` | PostgreSQL |
| `ADMIN_LOGIN` / `ADMIN_PASSWORD_HASH` | вход в CRM |
| `FLASK_SECRET_KEY` | сессии и производные секреты |
| `ADMIN_CHAT_ID` | куда бот шлёт уведомления (можно несколько ID через запятую) |
| `WEBHOOK_URL` | публичный HTTPS origin, например `https://your-domain.com` |
| `WEBHOOK_PATH` | путь webhook, по умолчанию `/webhook` |
| `WEBHOOK_ENFORCE_SECRET` | `1` — проверка секрета Telegram |
| `RUN_BOT_IN_BACKGROUND` | `1` — бот внутри процесса CRM (удобно на одном инстансе) |
| `REDIS_URL` | общий кэш/лимиты при нескольких процессах |
| `UNPAID_PAYMENT_TIMEOUT_MINUTES` | автоотмена неоплаченной заявки |
| `DATA_RETENTION_DAYS` | срок хранения старых записей/логов |

## Деплой

### Один сервис (Flask + бот в фоне)

Подходит для небольшого клуба на VPS или PaaS (Render и аналоги).

1. Выставьте переменные окружения по `.env.example`.
2. Выполните `python init_db.py`.
3. Запускайте **один** worker gunicorn (несколько workers без Redis дадут дубли бота):

```bash
gunicorn --workers 1 --worker-class gthread --threads 4 --timeout 60 --bind 0.0.0.0:8000 app:app
```

Конфиг `gunicorn.conf.py` и `Procfile` уже подготовлены под такой режим.

4. Укажите `WEBHOOK_URL=https://ваш-домен`. При старте приложение само ставит webhook; при необходимости вручную:

```bash
curl -X POST "https://api.telegram.org/bot<BOT_TOKEN>/setWebhook" \
  -d "url=https://ваш-домен/webhook" \
  -d "secret_token=<WEBHOOK_SECRET_TOKEN>"
```

5. Проверка: `GET /health` → `{"status":"ok",...}`, `GET /health/bot` → HTTP 200, если бот готов.

### VPS + systemd (кратко)

```bash
# после clone, venv, .env, init_db.py
sudo tee /etc/systemd/system/padelcrm.service >/dev/null <<'EOF'
[Unit]
Description=Padel Club CRM
After=network.target

[Service]
WorkingDirectory=/var/www/padel_mvp
Environment=PATH=/var/www/padel_mvp/venv/bin
EnvironmentFile=/var/www/padel_mvp/.env
ExecStart=/var/www/padel_mvp/venv/bin/gunicorn --workers 1 --worker-class gthread --threads 4 --timeout 60 --bind 0.0.0.0:8000 app:app
Restart=always
User=www-data

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now padelcrm
```

Перед nginx/HTTPS заверните порт 8000. Пример есть в стандартных гайдах по reverse proxy; заголовки `X-Forwarded-*` и `TRUST_PROXY=1` нужны для корректных cookie/HTTPS.

### Docker

В репозитории есть `Dockerfile` и `docker-compose.yml`. Для продакшена с отдельным процессом бота задайте `RUN_BOT_IN_BACKGROUND=0` и поднимайте второй сервис с `python bot.py`.

## Эксплуатация

- **Новая заявка** — раздел «Заявки»; при необходимости подтвердите статус.
- **Оплата** — игрок отмечает оплату в боте → уведомление админу → «Подтвердить» в «Оплаты».
- **Недобор** — за ~3 часа предупреждение, за ~1 час до старта незаполненные игры могут быть отменены автоматически (тренировки — по отдельной логике).
- **Привязка админа в Telegram** — предпочтительно `ADMIN_CHAT_ID` в env; запасной вариант: `/bindadmin <ADMIN_BIND_TOKEN>` (токен ≥ 24 символов).
- **Журнал** — действия в CRM.

## Безопасность

- не коммитьте `.env`;
- в продакшене только `ADMIN_PASSWORD_HASH`, не открытый пароль;
- держите `WEBHOOK_ENFORCE_SECRET=1`;
- регулярно меняйте `FLASK_SECRET_KEY`, токен бота и пароль CRM при передаче системы другому владельцу;
- делайте резервные копии PostgreSQL (pg_dump) перед обновлениями.

## Устранение неполадок

| Симптом | Что проверить |
|---|---|
| Бот молчит | `BOT_TOKEN`, логи процесса, `/health/bot`, что webhook указывает на ваш домен |
| CRM не пускает | `ADMIN_LOGIN` / `ADMIN_PASSWORD_HASH`, лимит логина |
| Ошибка БД | `DATABASE_URL`, доступ с сервера к Postgres |
| Дубли уведомлений | не больше одного worker при `RUN_BOT_IN_BACKGROUND=1` |
| Старый список игр | при раздельных процессах задайте `REDIS_URL` |

## Лицензия и передача

Исходный код передаётся вместе с этим руководством. Перед передачей новому владельцу смените все секреты, токен бота и пароли и очистите тестовые данные в базе.
