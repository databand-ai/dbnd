# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from pathlib import Path
from typing import List

import six

from dbnd import task
from targets import Target
from targets.types import PathStr


logger = logging.getLogger(__name__)


@task
def f_task_parameters(
    v_int: int,
    v_bool: bool,
    v_str: str,
    v_datetime: datetime.datetime,
    v_date: datetime.date,
    v_period: datetime.timedelta,
    v_list: List,
    v_list_obj: List[object],
    v_list_str: List[str],
    v_list_int: List[int],
    v_list_date: List[datetime.date],
) -> object:
    result = (
        v_int,
        v_datetime,
        v_str,
        v_date,
        v_period,
        v_list,
        v_list_obj,
        v_list_str,
        v_list_int,
        v_list_date,
    )
    logger.info("v_int: %s", v_int)
    logger.info("v_bool: %s", v_bool)
    logger.info("v_datetime: %s", v_datetime)
    logger.info("v_list: %s", v_list)
    logger.info("v_list_date: %s", v_list_date)

    return result


@task
def f_path_types(
    pathlib_path: Path, str_as_path: PathStr, target_path: Target
) -> object:
    assert isinstance(pathlib_path, Path)
    assert isinstance(str_as_path, six.string_types)
    assert isinstance(target_path, Target)

    return (pathlib_path, str_as_path, target_path)
