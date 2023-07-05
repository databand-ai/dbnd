# © Copyright Databand.ai, an IBM Company 2022

from dbnd import PipelineTask, PythonTask, data, output
from dbnd_run.tasks import DataSourceTask
from dbnd_run.testing.helpers import assert_run_task
from dbnd_test_scenarios import scenario_path
from targets import target


class TLogInputs(DataSourceTask):
    log = output

    def band(self):
        self.log = target(scenario_path("data/some_log.txt"))


class TLogFileReader(PythonTask):
    log = data
    processed_logs = output

    def run(self):
        self.processed_logs.write(self.log.read())


class TLogPipeline(PipelineTask):
    processed_logs = output

    def band(self):
        self.processed_logs = TLogFileReader(log=TLogInputs().log).processed_logs


class TestTaskDataSources(object):
    def test_external_task(self):
        task = TLogPipeline()
        assert_run_task(task)
