# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from pytest import fixture

from dbnd import PipelineTask, Task, data, output, parameter
from dbnd._core.utils.project.project_fs import project_path
from dbnd_spark.spark import SparkTask


logger = logging.getLogger(__name__)


@fixture
def databand_context_kwargs():
    return dict(conf={"local": {"root": "/some_path"}, "databand": {"verbose": "True"}})


task_target_date = datetime.date(year=2012, month=1, day=1)


def _sig(task):
    name = "signature %s" % task.task_name

    logger.info(task.ctrl.banner(name))
    for child in task.descendants.get_children():
        logger.info(child.ctrl.banner(name))

    return task.task_signature


def assert_signatures(tasks, expected):
    sigs = {t.task_name: _sig(t) for t in tasks}
    logger.warning("expected:\n%s\n" % expected)
    logger.warning("actual:\n%s\n" % sigs)
    assert expected == sigs


class TData(Task):
    t_param = data(default="foo")
    t_param2 = parameter.default(1)[int]
    t_param3 = parameter(default=False)[bool]

    t_output = output


class TSpark(SparkTask):
    t_param = data
    t_param2 = parameter[int]

    t_output = output


class TPySpark(SparkTask):
    t_param = data
    t_param2 = parameter[int]

    python_script = project_path("foo.py")

    t_output = output


class TPipeline(PipelineTask):
    t_param = parameter(default="foo")[str]

    t_o_spark = output
    t_o_pyspark = output

    def band(self):
        data = TData(t_param=self.t_param, t_param2=1, t_param3=False)
        self.t_o_spark = TSpark(t_param=data, t_param2=1)
        self.t_o_pyspark = TPySpark(t_param=data, t_param2=1)


class TestSparkTaskSignature(object):
    """
    This test checks that our signatures are not changed at different dbnd version
    Signature is a base component of task output path, and task outputs are the way for us to check
    if task is completed.
    If we change the signature, all tasks ran by the user before the change are going to be invalidated
    and if user re-run the pipeline/task - it will be executed again

    !!!Change these signature only if you know what you are doing! (don't just fix the test)!!!
    """

    def test_signatures_tasks(self):
        tasks = [
            TData(task_target_date=task_target_date),
            TSpark(t_param="a", t_param2=2, task_target_date=task_target_date),
            TPySpark(t_param="a", t_param2=2, task_target_date=task_target_date),
        ]
        # !!!Change these signature only if you know what you are doing! (don't just fix the test)!!!
        expected = {
            "TData": "29c9aa51d4",
            "TSpark": "2d9da6ad2d",
            "TPySpark": "49a709d191",
        }
        assert_signatures(tasks, expected)

    def test_signatures_pipeline(self):
        tasks = [TPipeline(task_target_date=task_target_date)]

        # !!!Change these signature only if you know what you are doing! (don't just fix the test)!!!
        expected = {"TPipeline": "2eddcc454b"}
        assert_signatures(tasks, expected)
