# © Copyright Databand.ai, an IBM Company 2022

import six

from dbnd import data
from dbnd_run.testing.helpers import assert_run_task
from dbnd_test_scenarios import scenario_path
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets import target
from targets.target_config import file


class TTextDataTask(TTask):
    text_data = data[str]

    def run(self):
        assert isinstance(self.text_data, six.string_types)
        assert self.text_data.startswith("mydata")
        super(TTextDataTask, self).run()


class TestTaskInputsFormats(object):
    def test_unknown_format(self):
        task = TTextDataTask(text_data=scenario_path("data/some_unknown_ext.myext"))
        assert_run_task(task)

    def test_pass_on_injected_format(self):
        task = TTextDataTask(
            text_data=target(
                scenario_path("data/some_unknown_ext.myext"), config=file.txt
            )
        )
        assert_run_task(task)
