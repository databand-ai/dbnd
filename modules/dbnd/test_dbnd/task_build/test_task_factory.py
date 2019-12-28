import logging

from dbnd import new_dbnd_context
from test_dbnd.factories import TTask, ttask_simple


logger = logging.getLogger(__name__)


class TestTaskMetaBuild(object):
    def test_verbose_build(self):
        with new_dbnd_context(conf={"task_build": {"verbose": "True"}}):
            task = TTask(override={TTask.t_param: "test_driver"})
            assert task.t_param == "test_driver"

    def test_sign_by_task_code_build(self):
        with new_dbnd_context(
            conf={"task_build": {"sign_with_full_qualified_name": "True"}}
        ):
            task = TTask()
            assert str(TTask.__module__) in task.task_meta.task_signature_source

    def test_task_call_source_class(self):
        task = TTask()
        logger.info(task.task_meta.task_call_source)
        assert task.task_meta.task_call_source
        assert task.task_meta.task_call_source[0].filename in __file__

    def test_task_call_source_func(self):
        task = ttask_simple.task()
        logger.info("SOURCE:%s", task.task_meta.task_call_source)
        assert task.task_meta.task_call_source[0].filename in __file__
