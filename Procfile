web: cd leadcrm && /opt/venv/bin/gunicorn leadcrm.wsgi --bind 0.0.0.0:$PORT --workers 4 --timeout 120
worker: cd leadcrm && /opt/venv/bin/celery -A leadcrm worker --loglevel=INFO --concurrency=4
cron: cd leadcrm && /opt/venv/bin/python manage.py run_scheduled_tasks
