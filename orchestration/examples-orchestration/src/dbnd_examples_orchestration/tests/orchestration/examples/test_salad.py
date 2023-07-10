# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples_orchestration.orchestration.examples.salad import prepare_salad

from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_prepare_salad(self):
        assert_run_task(prepare_salad.task())
