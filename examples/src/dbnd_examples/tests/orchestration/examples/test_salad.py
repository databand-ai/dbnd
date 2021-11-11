import logging

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.examples.salad import prepare_salad


logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_prepare_salad(self):
        assert_run_task(prepare_salad.task())
