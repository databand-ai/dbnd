# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.tutorial_syntax.T12_data_pandas_and_numpy import (
    f_test_pandas_numpy_flow,
)


logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_pandas_numpy(self):
        assert_run_task(f_test_pandas_numpy_flow.task())
