import datetime
import logging
import time

from dbnd import pipeline, task
from dbnd.utils import data_combine, period_dates


logger = logging.getLogger(__name__)


@task
def fetch_wine_quality(task_target_date: datetime.date, fail_task: bool = False):
    if not fail_task:
        logger.info("Sleeping for 120 seconds")
        time.sleep(120)
    else:
        time.sleep(5)
        raise Exception("User exception")
    return task_target_date


@pipeline
def fetch_data(task_target_date, period=datetime.timedelta(days=7)):
    all_data = []
    fail_task = True
    for d in period_dates(task_target_date, period):
        if fail_task:
            data = fetch_wine_quality(task_target_date=d, fail_task=True)
            fail_task = False
        else:
            data = fetch_wine_quality(task_target_date=d)
        all_data.append(data)

    return data_combine(all_data, sort=True)


@pipeline
def fetch_data_error():
    data = fetch_data()
    return data
