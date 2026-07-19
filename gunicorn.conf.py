"""Gunicorn config for Render: start Telegram bot inside each worker.

Bot must live in the same process that serves /webhook. Starting it at
module import under the master (or before fork) leaves workers with
bot_ready=false.

Important: post_worker_init must return quickly. If it blocks, Render's
healthCheckPath=/health never gets a response → white screen / failed cron.
"""
import os

workers = 1
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "4"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))
graceful_timeout = 30
# Реже recycle — каждый рестарт даёт окно «bot not ready» на кнопки Telegram.
max_requests = int(os.getenv("GUNICORN_MAX_REQUESTS", "1000"))
max_requests_jitter = int(os.getenv("GUNICORN_MAX_REQUESTS_JITTER", "100"))
bind = f"0.0.0.0:{os.getenv('PORT', '10000')}"
# Avoid false WORKER TIMEOUTs on slow disks (Render). Skip if /dev/shm missing.
if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK):
    worker_tmp_dir = "/dev/shm"
preload_app = False


def post_worker_init(worker):
    """Called in the worker process after boot — safe place to start the bot.

    Never block here: start_background_services() only spawns daemon threads.
    """
    import app as app_module

    app_module.logger.warning("gunicorn post_worker_init: starting background services")
    try:
        app_module.start_infra_services()
        app_module.start_background_services()
    except Exception:
        app_module.logger.exception("gunicorn post_worker_init failed")
