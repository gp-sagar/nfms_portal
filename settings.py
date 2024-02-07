from datetime import datetime, timedelta
today_date = datetime.now()

CELERY_IMPORTS = ('apps.BlockloadAPI', 'apps.DailyloadAPI')

API_HEADER = {
    "Content-Type": "application/json",
    "Custom-Header": "header-value",
}

RETRY_INTERVAL = [30, 60, 120] 

def get_time_shift():
    end_date = today_date.strftime('%Y-%m-%d')
    start_date = (today_date - timedelta(days=1)).replace(hour=0, minute=30, second=0, microsecond=0).strftime('%Y-%m-%d')
    time_shift = [f'{start_date} {hour:02d}:{minute:02d}:00' for hour in range(0, 24) for minute in [0, 30]]
    time_shift.append(f'{end_date} 00:00:00')
    return time_shift