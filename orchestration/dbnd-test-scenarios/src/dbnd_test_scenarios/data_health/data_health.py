# © Copyright Databand.ai, an IBM Company 2022

import os

from random import randint, randrange, uniform
from time import sleep

import numpy as np
import pandas as pd

from dbnd import dataset_op_logger, dbnd_tracking, log_dataset_op, task
from dbnd._core.constants import DbndDatasetOperationType


"""
run with `python dbnd-core/orchestration/dbnd-test-scenarios/src/dbnd_test_scenarios/data_health/data_health.py`
"""


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + pd.Timedelta(seconds=random_second)


@task()
def get_data(source: str, days: int):
    today = pd.Timestamp.now().today()
    start_date = today - pd.Timedelta(days=days)

    name = (
        f"test_data_{today.strftime('%Y-%m-%d')}_{start_date.strftime('%Y-%m-%d')}.csv"
    )
    file_path = os.path.join(source, name)

    records_amounts = int((days * 24 * 10) * uniform(0.5, 1.5))
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(records_amounts, 4)), columns=list("ABCD")
    )
    df["dates"] = [random_date(start_date, today) for _ in range(len(df))]

    log_dataset_op(
        op_path=file_path,
        op_type=DbndDatasetOperationType.read,
        data=df,
        with_schema=True,
        with_preview=True,
        with_histograms=True,
    )
    return df


@task()
def manipulate(df):
    drop_indices = np.random.choice(df.index, randint(10, len(df)), replace=False)
    df_subset = df.drop(drop_indices)
    return df_subset


@task()
def write_data(dest_dir, df):
    for date_time, grouped in df.groupby(pd.Grouper(key="dates", freq="H")):
        file_path = os.path.join(
            dest_dir,
            "date={}/time={}".format(
                date_time.strftime("%Y%m%d"), date_time.strftime("%H%M%S")
            ),
            "data_data_x.parquet",
        )

        if not grouped.empty:
            try:
                with dataset_op_logger(
                    op_path=file_path, op_type="write", data=grouped, send_metrics=False
                ):
                    randomly_fail()
            except:
                pass


@task()
def main(source, dest, days, index):
    randomly_fail(index)

    df = get_data(source, days=days)
    randomly_fail(index)

    df = manipulate(df)
    randomly_fail(index)

    write_data(dest, df)
    randomly_fail(index)


def randomly_fail(index):
    if (index % 2) != 0:
        raise Exception


if __name__ == "__main__":
    # run with `python  dbnd-core/orchestration/dbnd-test-scenarios/src/dbnd_test_scenarios/data_health/data_health.py`
    for index in range(20):
        try:
            with dbnd_tracking():
                main(
                    f"/tmp/input/test_data",
                    f"/tmp/output/test_data/",
                    days=3,
                    index=index,
                )
        except Exception:
            pass

        sleep(0.3)
