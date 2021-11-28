import logging

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.customizations.custom_target import CustomIOPipeline


logger = logging.getLogger(__name__)


class TestRunWithCustomTargets(object):
    def test_parameter_with_multiple_partitions(self):
        assert_run_task(CustomIOPipeline())
