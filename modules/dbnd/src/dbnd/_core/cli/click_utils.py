# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.utils.string_utils import strip_whitespace


logger = logging.getLogger(__name__)


def _help(string):
    if not string:
        return "-"
    return strip_whitespace(string)
