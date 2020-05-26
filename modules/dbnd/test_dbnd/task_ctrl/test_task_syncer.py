from typing import List
from unittest.mock import MagicMock, Mock

import mock

import targets

from dbnd import Task, config, dbnd_config, new_dbnd_context, pipeline, task
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.parameter.parameter_builder import output
from dbnd._core.task_ctrl.task_meta import TaskMeta
from dbnd._core.task_ctrl.task_parameters import TaskParameters
from dbnd._core.task_ctrl.task_syncer import TaskSyncer
from targets import LocalFileSystem, Target
from targets.fs import register_file_system
from targets.target_config import TargetConfig


def save_my_custom_object(path: str, data) -> None:
    with open(path, "w") as fd:
        fd.writelines(data)


def load_my_custom_object(path: str) -> List[str]:
    with open(path, "r") as fd:
        return fd.readlines()


@task
def read(loc: Target) -> str:
    data = load_my_custom_object(loc.path)
    return " ".join(data)


@task
def write(data, res=output.data.require_local_access):
    save_my_custom_object(res, data)


@pipeline
def write_read(data=["abcd", "zxcvb"]):
    path = write(data)
    r = read(path.res)
    return r


class TestTaskSyncer:
    def test_task_syncer_pre_exec(self):
        params = MagicMock(TaskParameters)
        task = MagicMock(Task)
        task_meta = MagicMock(TaskMeta)
        task_meta.configure_mock(dbnd_context=MagicMock(DatabandContext))
        task.configure_mock(task_meta=task_meta, _params=params)
        task_syncer = TaskSyncer(task)
        task_syncer.sync_pre_execute()
