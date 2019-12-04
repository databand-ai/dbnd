import logging

from dbnd._core.task_build.task_context import current, has_current_task


logger = logging.getLogger(__name__)


def log_dataframe(key, value):
    if not has_current_task():
        logger.info("DataFrame Shape '{}'='{}'".format(key, value.shape))
        return
    return current().log_dataframe(key, value)


def log_metric(key, value):
    if not has_current_task():
        logger.info("Metric '{}'='{}'".format(key, value))
        return
    return current().log_metric(key, value)


def log_artifact(key, artifact):
    if not has_current_task():
        logger.info("Artifact %s=%s", key, artifact)
        return
    return current().log_artifact(key, artifact)
