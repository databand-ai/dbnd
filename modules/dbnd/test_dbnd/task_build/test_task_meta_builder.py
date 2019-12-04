import logging

from dbnd import new_dbnd_context
from test_dbnd.factories import TTask


logger = logging.getLogger(__name__)


class TestTaskMetaBuild(object):
    def test_verbose_build(self):
        with new_dbnd_context(conf={"task_build": {"verbose": "True"}}):
            task = TTask(override={TTask.t_param: "test_driver"})
            assert task.t_param == "test_driver"
