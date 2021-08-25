import logging

import pytest
import six

from dbnd import config
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.data import data_repo


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.orchestration.examples.wine_quality.wine_quality_simple import (
        predict_wine_quality,
    )
    from dbnd_examples.orchestration.examples.wine_quality.wine_quality_script import (
        my_training_script,
    )

logger = logging.getLogger(__name__)


class TestQuickStartExample(object):
    def test_predict_wine_quality(self):
        task_config = {"raw_data": data_repo.wines_full}
        config.set_values(
            {
                predict_wine_quality.task_cls.task_definition.task_config_section: task_config
            },
            source="--set-root",
        )
        assert_run_task(predict_wine_quality.task())

    def test_script(self):
        result = my_training_script()
        assert result is not None
