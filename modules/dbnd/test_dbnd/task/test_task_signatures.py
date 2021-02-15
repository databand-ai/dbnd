import datetime
import logging

from collections import OrderedDict
from typing import Dict

from pytest import fixture

from dbnd import PipelineTask, PythonTask, data, output, parameter
from dbnd._core.current import get_databand_context
from dbnd._core.run.databand_run import new_databand_run
from dbnd.tasks import Task


logger = logging.getLogger(__name__)


@fixture
def databand_context_kwargs():
    return dict(conf={"local": {"root": "/some_path"}, "databand": {"verbose": "True"}})


task_target_date = datetime.date(year=2012, month=1, day=1)


def _sig(task):
    with new_databand_run(context=get_databand_context(), job_name=task.task_name):
        for child in [task] + list(task.descendants.get_children()):
            name = "signature %s" % child.task_name
            logger.info(child.ctrl.banner(name))

    return task.task_signature


def assert_signatures(tasks, expected):
    sigs = {t.task_name: _sig(t) for t in tasks}
    print("expected:\n%s\n" % expected)
    print("actual:\n%s\n" % sigs)
    assert expected == sigs


class TData(Task):
    t_param = data(default="foo")
    t_param2 = parameter.default(1)[int]
    t_param3 = parameter(default=False)[bool]

    t_output = output


class TPython(PythonTask):
    t_param = data
    t_param2 = parameter[int]

    t_output = output


class TPipeline(PipelineTask):
    t_param = parameter(default="foo")[str]
    t_o_python = output

    def band(self):
        data = TData(t_param=self.t_param, t_param2=1, t_param3=False)
        self.t_o_python = TPython(t_param=data, t_param2=1)


class TestTaskSignature(object):
    """
    This test checks that our signatures are not changed at different dbnd version
    Signature is a base component of task output path, and task outputs are the way for us to check
    if task is completed.
    If we change the signature, all tasks ran by the user before the change are going to be invalidated
    and if user re-run the pipeline/task - it will be executed again

    !!!Change these signature only if you know what you are doing! (don't just fix the test)!!!
    """

    def test_task_id_stable(self):
        class TPython(PythonTask):
            t_param = parameter[Dict]

        first_sig = _sig(
            TPython(t_param=dict(a=1, b=2, c=""), task_target_date=task_target_date)
        )
        assert first_sig == _sig(
            TPython(t_param=dict(b=2, c="", a=1), task_target_date=task_target_date)
        )
        assert first_sig == _sig(
            TPython(
                t_param=OrderedDict(c="", b=2, a=1), task_target_date=task_target_date
            )
        )

    def test_signatures_tasks(self):
        tasks = [
            TData(task_target_date=task_target_date),
            TPython(t_param="a", t_param2=2, task_target_date=task_target_date),
        ]

        # !!!Change these signature only if you know what you are doing! (don't just fix the test)!!!
        expected = {"TData": "29c9aa51d4", "TPython": "4a13659ca5"}
        assert_signatures(tasks, expected)

    def test_signatures_pipeline(self):
        tasks = [TPipeline(task_target_date=task_target_date)]

        # !!!Change these signature only if you know what you are doing! (don't just fix the test)!!!
        expected = {"TPipeline": "a14cf2517b"}
        assert_signatures(tasks, expected)
