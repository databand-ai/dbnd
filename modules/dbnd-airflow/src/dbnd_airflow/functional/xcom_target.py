# © Copyright Databand.ai, an IBM Company 2022

import io

from typing import List, Tuple

from dbnd_airflow.airflow_utils import safe_get_context_manager_dag
from targets import AtomicLocalFile
from targets.fs import register_file_system
from targets.fs.file_system import FileSystem
from targets.pipes.base import FileWrapper


def build_xcom_str(op, name=None):
    """
    :param op: airflow operator
    :param name: name of result
    :return:
    """
    result_key = "['%s']" % name if name else ""
    xcom_path = "{{task_instance.xcom_pull('%s')%s}}" % (op.task_id, result_key)
    return XComStr(xcom_path, op.task_id)


class _BaseOperator(object):
    """
    Abstract class implementing down and up stream operations.
    """

    @property
    def op(self):
        raise NotImplemented()

    def set_downstream(self, task_or_task_list):
        self.op.set_downstream(task_or_task_list)

    def set_upstream(self, task_or_task_list):
        self.op.set_upstream(task_or_task_list)

    def __lshift__(self, other):
        return self.set_upstream(other)

    def __rshift__(self, other):
        return self.set_downstream(other)


class XComStr(str, _BaseOperator):
    def __new__(cls, value, task_id):
        # explicitly only pass value to the str constructor
        obj = super(XComStr, cls).__new__(cls, value)
        obj.task_id = task_id
        return obj

    def __iter__(self):
        raise TypeError(
            "It seems that you are trying to assign output to multiple values. If your task's function "
            "returns multiple values please add type annotations to it."
        )

    @property
    def op(self):
        return safe_get_context_manager_dag().get_task(self.task_id)


class XComResults(_BaseOperator):
    target_no_traverse = True

    def __init__(self, result, sub_results):
        # type: (XComStr, List[ Tuple[str, XComStr]]) -> XComResults
        self.xcom_result = result
        self.sub_results = sub_results
        self._names = [n for n, _ in sub_results]
        for k, v in self.sub_results:
            if not hasattr(self, k):
                setattr(self, k, v)

    @property
    def names(self):
        return self._names

    def __iter__(self):
        return iter(xcom_sub_result for _, xcom_sub_result in self.sub_results)

    def __repr__(self):
        return "result(%s)" % ",".join(self.names)

    def as_dict(self):
        return dict(self.sub_results)

    @property
    def op(self):
        return safe_get_context_manager_dag().get_task(self.sub_results[0][1].task_id)

    def __getitem__(self, key):
        return self.as_dict()[key]


class AirflowXComFileSystem(FileSystem):
    def exists(self, path):
        return False

    def open_read(self, path, mode="r"):
        return FileWrapper(io.BufferedReader(io.FileIO(path, mode)))

    def open_write(self, path, mode="w", **kwargs):
        return AtomicLocalFile(path, fs=self, mode=mode)


class XComAtomicFile(AtomicLocalFile):
    def move_to_final_destination(self):
        self.fs.move_from_local(self.tmp_path, self.path)


register_file_system("xcom", AirflowXComFileSystem)
register_file_system("jinja", AirflowXComFileSystem)
