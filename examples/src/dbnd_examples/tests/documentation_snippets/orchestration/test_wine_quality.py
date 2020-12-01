import logging

import pytest
import six

from dbnd.testing.helpers_pytest import assert_run_task


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.pipelines.wine_quality.wine_quality_decorators_py3 import (
        predict_wine_quality,
        predict_wine_quality_parameter_search,
    )

logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_predict_wine_quality(self):
        assert_run_task(predict_wine_quality.task())

    def test_parameter_search(self):
        assert_run_task(predict_wine_quality_parameter_search.task())
