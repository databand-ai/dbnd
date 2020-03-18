import logging
import typing

from dbnd._core.task_build.task_context import current, has_current_task
from targets.values import get_value_type_of_obj


if typing.TYPE_CHECKING:
    from typing import Optional, Union
    import pandas as pd
    import pyspark.sql as spark


logger = logging.getLogger(__name__)


def log_dataframe(key, value, with_preview=True):
    # type: (str, Union[pd.DataFrame, spark.DataFrame], Optional[bool]) -> None
    if not has_current_task():
        value_type = get_value_type_of_obj(value)
        shape = value_type.get_data_dimensions(value)
        logger.info("DataFrame Shape '{}'='{}'".format(key, shape))
        return

    return current().log_dataframe(key, value, with_preview)


def log_metric(key, value, source="user"):
    if not has_current_task():
        logger.info("{} Metric '{}'='{}'".format(source.capitalize(), key, value))
        return
    return current().log_metric(key, value, source=source)


def log_artifact(key, artifact):
    if not has_current_task():
        logger.info("Artifact %s=%s", key, artifact)
        return
    return current().log_artifact(key, artifact)
