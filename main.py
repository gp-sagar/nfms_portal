import os
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv
from apps.BlockloadAPI import BlockloadTask
# from apps.DailyloadAPI import DailyloadTask
# from apps.BillingAPI import BillingTask
load_dotenv()

app = Celery('your_app_name', broker=os.getenv('CELERY_BROKER_URL'), backend=os.getenv('CELERY_RESULT_BACKEND'))
app.config_from_object('settings')

# Configure periodic tasks
app.conf.beat_schedule = {
    'BlockloadAPI-task': {
        'task': 'apps.BlockloadAPI.BlockloadTask',
        'schedule': crontab(minute='*/1'),
    },
    'DailyloadAPI-task': {
        'task': 'apps.DailyloadAPI.DailyloadTask',
        'schedule': crontab(minute='*/1'),
    },
    'BillingAPI-task': {
        'task': 'apps.BillingAPI.BillingTask',
        'schedule': crontab(minute='*/1'),
    },
}
BlockloadTask()
# DailyloadTask()
# BillingTask()