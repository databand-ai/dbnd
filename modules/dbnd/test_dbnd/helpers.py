# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import logging


logger = logging.getLogger(__name__)


def raise_example_failure(message):
    def _user_failure_internal():
        raise Exception(message)

    _user_failure_internal()
