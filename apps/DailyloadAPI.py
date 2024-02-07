from main import app

@app.task
def my_task():
    print("DailyloadAPI fetchnig...")