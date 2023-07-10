# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples_orchestration.orchestration.customizations.custom_target import (
    CustomIOPipeline,
)

from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestRunWithCustomTargets(object):
    def test_parameter_with_multiple_partitions(self):
        assert_run_task(CustomIOPipeline())
