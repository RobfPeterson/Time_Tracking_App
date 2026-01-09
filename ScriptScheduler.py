import schedule
import time
import lossfunction
from datetime import datetime

def run_scheduled_tasks():
    print("Scheduled tasks have been set up.")
    print(f"Current time: {datetime.now()}")
    schedule.every().day.at("21:00", "America/Los_Angeles").do(lossfunction.main)
    while True:
        schedule.run_pending()
        time.sleep(1)
if __name__ == "__main__":
    run_scheduled_tasks()
