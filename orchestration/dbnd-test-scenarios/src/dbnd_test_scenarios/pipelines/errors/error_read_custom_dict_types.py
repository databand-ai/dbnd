# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict

from dbnd import band, task


logger = logging.getLogger(__name__)


class _CustomType(object):
    pass


@task
def t_return_custom_dict():
    # type:()->Dict
    return {1: _CustomType()}


@task
def t_read_custom_dict(d):
    # type:(Dict)->str
    return "ok"


@band
def t_band():
    a = t_return_custom_dict()
    return t_read_custom_dict(a)
