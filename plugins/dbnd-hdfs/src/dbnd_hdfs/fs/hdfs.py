from dbnd import Config, dbnd_context, parameter
from dbnd._core.task_build.task_registry import build_task_from_config
from targets import FileSystem


def create_hdfs_client():  # type ()-> FileSystem
    return build_task_from_config(dbnd_context().env.hdfs)
