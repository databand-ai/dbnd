# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples_orchestration.orchestration.tutorial_syntax.T12_data_pandas_and_numpy import (
    f_test_pandas_numpy_flow,
)

from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_pandas_numpy(self):
        assert_run_task(f_test_pandas_numpy_flow.task())
