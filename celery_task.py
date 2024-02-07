# main.py
import os
from celery import Celery
from celery.schedules import crontab
from celery.schedules import schedule

from dotenv import load_dotenv
# from apps.BlockloadAPI import BlockloadTask
load_dotenv()

app = Celery('celery_app', broker=os.getenv('CELERY_BROKER_URL'), backend=os.getenv('CELERY_RESULT_BACKEND'), include = ['apps.BlockloadAPI'])

app.conf.update(
    result_expires=3600,
    timezone='UTC',
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1,
)

# app.config_from_object('settings')

# Configure periodic tasks
app.conf.beat_schedule = {
    'call_blockload_api_every_30_seconds': {
        'task': 'BlockloadTask',
        'schedule': crontab(minute='*/1'),
    },
    # 'APICall-task': {
    #     'task': 'celery_app.BlockloadAPI.BlockloadAPICall',
    #     'schedule': crontab(minute='*/1'),
    # },
}

if __name__ == '__main__':
    app.start()