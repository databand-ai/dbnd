# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import logging

import six

from targets.marshalling.marshaller import Marshaller
from targets.marshalling.marshaller_ctrl import get_marshaller_ctrl
from targets.marshalling.marshaller_loader import MarshallerLoader


logger = logging.getLogger(__name__)

__all__ = ["get_marshaller_ctrl", "Marshaller", "MarshallerLoader"]


def register_marshaller(value_type_or_obj_type, file_format, marshaller):
    from targets.values import get_value_type_of_type

    value_type = get_value_type_of_type(value_type_or_obj_type)
    if not value_type:
        raise Exception("register %s first" % value_type_or_obj_type)
    value_type.register_marshaller(file_format, marshaller)


def register_marshallers(value_type_or_obj_type, marshaller_dict):
    from targets.values import get_value_type_of_type

    value_type = get_value_type_of_type(value_type_or_obj_type)
    if not value_type:
        raise Exception("register %s first" % value_type_or_obj_type)
    for file_format, marshaller_cls in six.iteritems(marshaller_dict):
        register_marshaller(value_type, file_format, marshaller_cls)
