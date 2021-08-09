from __future__ import absolute_import

import logging

from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


def value_at_task(parameter):
    """
    A hackish way to get the "value" of a parameter.
    """

    class _DummyTask(TTask):
        param = parameter

    return _DummyTask().param


def raise_example_failure(message):
    def _user_failure_internal():
        raise Exception(message)

    _user_failure_internal()
