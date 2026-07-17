web: gunicorn --workers 1 --worker-class gthread --threads 2 --timeout 120 --graceful-timeout 30 --max-requests 150 --max-requests-jitter 30 --worker-tmp-dir /dev/shm app:app
