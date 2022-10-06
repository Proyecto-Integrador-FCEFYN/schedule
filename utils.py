# https://schedule.readthedocs.io/en/stable/parallel-execution.html
import os
import threading
import time
import schedule
from db import DatabaseConnection
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta


mongo_url = os.getenv('MONGO_URL', "mongodb://roberto:sanchez@150.136.250.71:27017/")
files_db = os.getenv('FILES_DB', "djongo")
event_db = os.getenv('EVENT_DB', "djongo")


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


class Schedule:

    def __init__(self, collection: str, db: DatabaseConnection):
        self.duration_collection = collection
        self.db = db
        self.__run()

    def __job(self):

        self.db.connect()
        doc = self.db.get_events_duration(self.duration_collection)
        year = doc['year']
        month = doc['month']
        now = dt.now()
        duration = now - relativedelta(months=month + month*year)
        print(dt.isoformat(duration))

        list_of_collections = [
            'events_button',
            'events_deniedaccess',
            'events_movement',
            'events_permittedaccess',
            'events_webopendoor'
        ]

        for collection in list_of_collections:
            self.db.delete_events_duration(collection=collection, duration=duration)
        self.db.delete_files_duration(duration=duration)

    def __run(self):
        days = os.getenv('CHECK_OLD_EVENT_DAYS', 15)
        # schedule.every(days).days.do(run_threaded, self.__job)
        schedule.every(1).minute.do(run_threaded, self.__job())
        while 1:
            schedule.run_pending()
            time.sleep(1)


def main():
    db = DatabaseConnection(conn_string=mongo_url, event_db=event_db, files_db=files_db)
    Schedule(collection='events_eventsduration', db=db)


if __name__ == "__main__":
    main()
