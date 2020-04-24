import datetime
import logging

from pathlib import Path
from typing import List

import six

from dbnd import pipeline, task
from targets import Target, target
from targets.types import PathStr


logger = logging.getLogger(__name__)


@task
def f_simple_types(v_int: int, v_bool: bool, v_str: str) -> str:
    assert isinstance(v_int, int)
    assert isinstance(v_bool, bool)
    assert isinstance(v_str, six.string_types)

    logger.info("v_int: %s", v_int)
    logger.info("v_bool: %s", v_bool)

    return _return_value(v_int, v_bool, v_str)


@task
def f_datetime_types(
    v_datetime: datetime.datetime, v_date: datetime.date, v_period: datetime.timedelta
) -> str:
    assert isinstance(v_datetime, datetime.datetime)
    assert isinstance(v_date, datetime.date)
    assert isinstance(v_period, datetime.timedelta)

    return _return_value(v_datetime, v_date, v_period)


@task
def f_path_types(pathlib_path: Path, str_as_path: PathStr, target_path: Target) -> str:
    assert isinstance(pathlib_path, Path)
    assert isinstance(str_as_path, six.string_types)
    assert isinstance(target_path, Target)

    return _return_value(pathlib_path, str_as_path, target_path)


@task
def f_lists(
    v_list: List,
    v_list_obj: List[object],
    v_list_str: List[str],
    v_list_int: List[int],
    v_list_date: List[datetime.date],
) -> str:
    assert isinstance(v_list_date[0], datetime.date)

    assert isinstance(v_list_obj[0], int)
    assert isinstance(v_list_obj[1], six.string_types)

    assert isinstance(v_list_int[0], int)
    assert isinstance(v_list_str[0], six.string_types)

    return _return_value(v_list, v_list_obj, v_list_str, v_list_date, v_list_int)


EXPECTED_STR = (
    "'1' 'False' 'strstr'",
    "'2018-01-01 00:00:00' '2018-01-01' '1 day, 0:00:00'",
)

some_existing_file = __file__


def _return_value(*values):
    v = " ".join(map(lambda x: "'%s'" % x, values))
    logger.info(v)
    return v


@pipeline
def f_run_all_simple():
    simple = f_simple_types(1, False, "strstr")
    dt = f_datetime_types(
        v_datetime=datetime.datetime(year=2018, month=1, day=1),
        v_date=datetime.date(year=2018, month=1, day=1),
        v_period=datetime.timedelta(days=1),
    )
    paths = f_path_types(
        pathlib_path=Path(some_existing_file),
        str_as_path=some_existing_file,
        target_path=target(some_existing_file),
    )

    lists = f_lists(
        v_list=[1, "1"],
        v_list_obj=[2, "2"],
        v_list_str=["str", "str"],
        v_list_int=[1, 2],
        v_list_date=[datetime.date(year=2018, month=1, day=1)],
    )

    return simple, dt, paths, lists


@task
def f_assert(v_simple, v_dt, v_paths=None, v_list=None):
    assert "'1' 'False' 'strstr'" == v_simple
    assert "'2018-01-01 00:00:00' '2018-01-01' '1 day, 0:00:00'" == v_dt
    f_paths = (some_existing_file, some_existing_file, some_existing_file)
    assert "'%s' '%s' '%s'" % f_paths == v_paths
    assert (
        "'[1, '1']' '[2, '2']' '['str', 'str']' '[datetime.date(2018, 1, 1)]' '[1, 2]'"
        == v_list
    )

    return "OK"


@pipeline
def f_test_flow():
    all_simple = f_run_all_simple()
    return f_assert(*all_simple)


if __name__ == "__main__":
    # we can just run the function!
    f_test_flow()
