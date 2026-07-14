# Деплой Padel MVP на VPS

## 1. Требования
- Ubuntu 22.04+ или любой Linux VPS
- Python 3.11+
- Nginx (по желанию, для HTTPS)
- домен с DNS-записью A -> IP VPS
- PostgreSQL / Neon (уже используется)

## 2. Подготовка сервера
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv nginx git curl ufw
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

## 3. Получение проекта
```bash
cd /var/www
sudo git clone <repo-url> padel_mvp
cd padel_mvp
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Заполни `.env` на сервере:
```env
BOT_TOKEN=...
ADMIN_CHAT_ID=...
DATABASE_URL=...
ADMIN_LOGIN=admin
ADMIN_PASSWORD=...
FLASK_SECRET_KEY=...
WEBHOOK_URL=https://your-domain.com
WEBHOOK_PATH=/webhook
RUN_BOT_IN_BACKGROUND=1
```

## 4. Инициализация базы
```bash
source venv/bin/activate
python init_db.py
```

## 5. Запуск через Gunicorn
```bash
source venv/bin/activate
gunicorn --workers 1 --bind 0.0.0.0:8000 app:app
```

> Важно: при `RUN_BOT_IN_BACKGROUND=1` бот запускается фоновым потоком
> внутри этого же процесса. Каждый gunicorn worker — отдельный процесс со
> своей памятью, поэтому при `--workers` больше 1 получится несколько
> параллельных ботов (дублирующиеся напоминания) и несинхронизированные
> кэши списка игр между воркерами. Если нужно больше воркеров для CRM —
> задайте `REDIS_URL` (общий кэш/rate-limit) и запускайте бота отдельным
> сервисом (`RUN_BOT_IN_BACKGROUND=0` + отдельный процесс `python bot.py`).

Для стабильности лучше запускать через systemd. Пример сервиса:
```bash
sudo tee /etc/systemd/system/padelcrm.service > /dev/null <<'EOF'
[Unit]
Description=Padel CRM service
After=network.target

[Service]
WorkingDirectory=/var/www/padel_mvp
Environment=PATH=/var/www/padel_mvp/venv/bin
ExecStart=/var/www/padel_mvp/venv/bin/gunicorn --workers 1 --bind 0.0.0.0:8000 app:app
Restart=always
User=www-data

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable padelcrm
sudo systemctl start padelcrm
```

## 6. Настройка вебхука Telegram
После того как приложение доступно по HTTPS, вызови:
```bash
curl -X POST "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook?url=https://your-domain.com/webhook"
```

Проверь:
```bash
curl https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getWebhookInfo
```

## 7. Nginx (опционально)
```bash
sudo tee /etc/nginx/sites-available/padel > /dev/null <<'EOF'
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/padel /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## 8. Проверка
- Открой `https://your-domain.com/health` — должен вернуть JSON `{"status":"ok"}`
- Открой CRM по домену, войди под админом
- Напиши боту `/start` в Telegram — должен откликаться

## Рекомендация
Если нужен более стабильный вариант, лучше использовать Docker Compose с двумя сервисами:
- `web` (Flask + Gunicorn)
- `bot` (aiogram worker)

Но для текущего MVP достаточно одного процесса Flask/Gunicorn + фонового бота, потому что бот уже запускается внутри app.py.
