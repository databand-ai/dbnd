from datetime import date

from dbnd import data, output, parameter
from dbnd._core.inline import run_cmd_locally, run_task
from dbnd.tasks import Task
from dbnd.tasks.basics.sanity import dbnd_sanity_check
from dbnd.testing import assert_run_task
from test_dbnd.scenarios.pipelines.advance_pipeline import TComplicatedPipeline
from test_dbnd.scenarios.pipelines.pipe_4tasks import MainPipeline


class TestDatabandSanity(object):
    def test_single_task(self, tmpdir_factory):
        class TestTask(Task):
            test_input = data
            p = parameter[str]
            d = parameter[date]
            param_from_config = parameter[date]

            a_output = output.data

            def run(self):
                self.a_output.write("ss")

        actual = TestTask(test_input=__file__, p="333", d=date(2018, 3, 4))
        assert actual.p == "333"
        run_task(actual)
        assert actual.a_output.read() == "ss"

    def test_scenario_4_simple(self, tmpdir_factory):
        run_cmd_locally([MainPipeline.get_task_family()])

    def test_complicated_pipeline(self):
        assert_run_task(TComplicatedPipeline())

    def test_sanity(self):
        assert_run_task(dbnd_sanity_check.task())
