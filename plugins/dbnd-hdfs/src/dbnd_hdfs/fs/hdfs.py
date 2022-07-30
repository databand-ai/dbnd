# © Copyright Databand.ai, an IBM Company 2022

import typing

from dbnd import dbnd_context
from dbnd._core.task_build.task_registry import build_task_from_config


if typing.TYPE_CHECKING:
    from targets import FileSystem


def create_hdfs_client():
    # type: () -> FileSystem
    return build_task_from_config(dbnd_context().env.hdfs)
