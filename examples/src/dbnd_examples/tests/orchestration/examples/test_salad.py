import logging

import pytest
import six

from dbnd.testing.helpers_pytest import assert_run_task


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.orchestration.examples.salad.salad import prepare_salad

logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_prepare_salad(self):
        assert_run_task(prepare_salad.task())
