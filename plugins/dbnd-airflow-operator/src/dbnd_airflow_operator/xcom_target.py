import io

from typing import List, Tuple

from dbnd_airflow_operator.airflow_utils import safe_get_context_manager_dag
from targets import AtomicLocalFile
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
        return safe_get_context_manager_dag().get_task(self.task_id)

    def set_downstream(self, task_or_task_list):
        self.op.set_downstream(task_or_task_list)


class XComResults(object):
    target_no_traverse = True

    def __init__(self, result, sub_results):
        # type: (List[ Tuple[str, XComStr]]) -> XComResults
        self.xcom_result = result
        self.sub_results = sub_results

    @property
    def names(self):
        return [n for n, _ in self.sub_results]

    def __iter__(self):
        for _, xcom_sub_result in self.sub_results:
            yield xcom_sub_result

    def __repr__(self):
        return "result(%s)" % ",".join(self.names)

    def as_dict(self):
        return dict(self.sub_results)

    @property
    def op(self):
        return safe_get_context_manager_dag().get_task(self.sub_results[0][1].task_id)

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
