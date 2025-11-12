web: cd leadcrm && gunicorn leadcrm.wsgi --bind 0.0.0.0:$PORT --workers 4 --timeout 120
worker: cd leadcrm && celery -A leadcrm worker --loglevel=INFO --concurrency=4
