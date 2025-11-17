"""
Celery application initialization for Lead CRM.

This module configures Celery to work with Django and enables
background task processing for data scraping operations.
"""

from __future__ import absolute_import, unicode_literals

import os

from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')

app = Celery('leadcrm')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Explicitly import data_pipeline tasks (not a Django app)
# Import the task_queue module to register the tasks
import data_pipeline.jobs.task_queue  # noqa: E402, F401


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task for testing Celery setup."""
    print(f'Request: {self.request!r}')
