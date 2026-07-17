"""Gunicorn config for Render: start Telegram bot inside each worker.

Bot must live in the same process that serves /webhook. Starting it at
module import under the master (or before fork) leaves workers with
bot_ready=false.
"""
import os

workers = 1
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "2"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))
graceful_timeout = 30
max_requests = 150
max_requests_jitter = 30
bind = f"0.0.0.0:{os.getenv('PORT', '10000')}"
# Avoid false WORKER TIMEOUTs on slow disks (Render).
worker_tmp_dir = "/dev/shm"
preload_app = False


def post_worker_init(worker):
    """Called in the worker process after boot — safe place to start the bot."""
    import app as app_module

    app_module.logger.warning("gunicorn post_worker_init: starting background services")
    app_module.start_background_services()
