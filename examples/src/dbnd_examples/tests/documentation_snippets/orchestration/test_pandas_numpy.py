import logging

import pytest
import six

from dbnd.testing.helpers_pytest import assert_run_task


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.tutorial_syntax.T24_function_with_pandas_numpy import (
        f_test_pandas_numpy_flow,
    )

logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_pandas_numpy(self):
        assert_run_task(f_test_pandas_numpy_flow.task())
