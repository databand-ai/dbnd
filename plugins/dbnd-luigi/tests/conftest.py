import datetime
import os

import luigi
import pytest

from luigi.date_interval import Custom

from tests.luigi_examples.multiple_input_output import (
    MyWrapperTask,
    MyWrapperTaskOutputFails,
    MyWrapperTaskRequiresFail,
    MyWrapperTaskRunFail,
    TaskB,
    TaskC,
)
from tests.luigi_examples.top_artists import (
    Streams,
    Top10Artists,
    Top10ArtistsOutputException,
    Top10ArtistsRequiresException,
    Top10ArtistsRunException,
)


def delete_task_output(luigi_task):
    outputs = luigi.task.flatten(luigi_task.output())
    all_deps = luigi_task.deps()
    for d in all_deps:
        outputs += luigi.task.flatten(d.output())
    for t in outputs:
        if t.exists():
            os.remove(t.path)


@pytest.fixture(autouse=True)
def date_a():
    return datetime.date(year=2019, month=1, day=1)


@pytest.fixture(autouse=True)
def date_b(date_a):
    return datetime.date(year=2019, month=1, day=3)


@pytest.fixture(autouse=True)
def date_interval(date_a, date_b):
    return Custom(date_a, date_b)


@pytest.fixture(autouse=True)
def streams(date_a):
    t = Streams(date=date_a)
    return t


@pytest.fixture(autouse=True)
def top10_artists(date_interval):
    return Top10Artists(date_interval=date_interval)


@pytest.fixture(autouse=True)
def top10_artists_run_error(date_interval):
    return Top10ArtistsRunException(date_interval=date_interval)


@pytest.fixture(autouse=True)
def top10_artists_requires_error(date_interval):
    return Top10ArtistsRequiresException(date_interval=date_interval)


@pytest.fixture(autouse=True)
def top10_artists_output_error(date_interval):
    return Top10ArtistsOutputException(date_interval=date_interval)


@pytest.fixture(autouse=True)
def task_c():
    return TaskC()


@pytest.fixture(autouse=True)
def task_b():
    return TaskB()


@pytest.fixture(autouse=True)
def wrapper_task():
    return MyWrapperTask()


@pytest.fixture(autouse=True)
def wrapper_task_run_fail():
    return MyWrapperTaskRunFail()


@pytest.fixture(autouse=True)
def wrapper_task_requires_fail():
    return MyWrapperTaskRequiresFail()


@pytest.fixture(autouse=True)
def wrapper_task_output_fail():
    return MyWrapperTaskOutputFails()
