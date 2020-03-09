import io

from typing import List, Tuple

from airflow import settings

from targets import AtomicLocalFile, DataTarget
from targets.fs import register_file_system
from targets.fs.file_system import FileSystem
from targets.pipes.base import FileWrapper


class XComStr(str):
    def __new__(cls, value, task_id):
        # explicitly only pass value to the str constructor
        obj = super(XComStr, cls).__new__(cls, value)
        obj.task_id = task_id
        return obj

    @property
    def op(self):
        return settings.CONTEXT_MANAGER_DAG.get_task(self.task_id)

    def set_downstream(self, task_or_task_list):
        self.op.set_downstream(task_or_task_list)


class XComResults(object):
    target_no_traverse = True

    def __init__(self, xcom_args):
        # type: (List[ Tuple[str, XComStr]]) -> XComResults
        self.xcom_args = xcom_args

    def names(self):
        return [n for n, _ in self.xcom_args]

    def __iter__(self):
        for _, xcom_arg in self.xcom_args:
            yield xcom_arg

    def __repr__(self):
        return "result(%s)" % ",".join(self.names)

    def as_dict(self):
        return dict(self.xcom_args)

    @property
    def op(self):
        return settings.CONTEXT_MANAGER_DAG.get_task(self.xcom_args[0][1].task_id)

    def set_downstream(self, task_or_task_list):
        self.op.set_downstream(task_or_task_list)


class AirflowXComFileSystem(FileSystem):
    def exists(self, path):
        return False

    def open_read(self, path, mode="r"):
        return FileWrapper(io.BufferedReader(io.FileIO(path, mode)))

    def open_write(self, path, mode="w"):
        return AtomicLocalFile(path, fs=self, mode=mode)


class XComAtomicFile(AtomicLocalFile):
    def move_to_final_destination(self):
        self.fs.move_from_local(self.tmp_path, self.path)


register_file_system("xcom", AirflowXComFileSystem)
