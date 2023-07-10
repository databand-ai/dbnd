# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples_orchestration.orchestration.tutorial_syntax import T03_task_outputs


logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_t23_task_with_multiple_outputs_v1(self):
        t = T03_task_outputs.f_returns_two_dataframes_v1.dbnd_run(1).task
        assert t.features

    def test_t23_task_with_multiple_outputs_v2(self):
        t = T03_task_outputs.f_returns_two_dataframes_v2.dbnd_run(2).task
        assert t.features

    def test_t23_task_with_multiple_outputs_no_hint(self):
        t = T03_task_outputs.f_returns_two_dataframes_no_hint.dbnd_run(2).task
        assert t.result_1

    def test_t23_task_with_multiple_outputs_named_tuple_v1(self):
        t = T03_task_outputs.f_returns_two_dataframes_named_tuple_v1.dbnd_run(2).task
        assert t.features

    def test_t23_task_with_multiple_outputs_named_tuple_v2(self):
        t = T03_task_outputs.f_returns_two_dataframes_named_tuple_v2.dbnd_run(2).task
        assert t.features
