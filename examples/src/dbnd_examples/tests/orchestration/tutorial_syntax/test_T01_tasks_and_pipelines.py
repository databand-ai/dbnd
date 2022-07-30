# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples.orchestration.tutorial_syntax.T01_tasks_and_pipelines import (
    calculate_values_pipeline,
    calculate_values_with_base_pipeline,
)


logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_pipe_operations_pipe(self):
        run_result = calculate_values_pipeline.dbnd_run()
        assert 2 == run_result.root_task.result[0].load(object)

    def test_pipe_operations_pipelines(self):
        run_result = calculate_values_with_base_pipeline.dbnd_run()
        assert 7 == run_result.root_task.result[2].load(object)
