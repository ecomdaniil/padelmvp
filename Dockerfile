FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

# --workers 1: при RUN_BOT_IN_BACKGROUND=1 бот работает потоком внутри
# этого процесса — 2+ воркеров = 2+ независимых бота/кэша/планировщика
# (см. docker-compose.yml). --worker-class gthread --threads 4 даёт
# конкурентность без второго процесса.
CMD ["gunicorn", "--workers", "1", "--worker-class", "gthread", "--threads", "4", "--timeout", "60", "--bind", "0.0.0.0:8000", "app:app"]
