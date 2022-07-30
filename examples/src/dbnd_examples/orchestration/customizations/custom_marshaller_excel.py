# © Copyright Databand.ai, an IBM Company 2022

"""
This is a way to add another format to serialize/deserialize SizedMessage example object
"""
import logging

import joblib

from dbnd import output, task
from targets.marshalling import register_marshaller
from targets.marshalling.marshaller import Marshaller
from targets.target_config import TargetConfig, register_file_extension
from targets.values import ValueType, register_value_type


logger = logging.getLogger(__name__)


class SizedMessage(object):
    def __init__(self, msg, size):
        self.msg = msg
        self.size = size


# This defines how SizedMessage is parsed and serialised into strings
class MessageValueType(ValueType):
    type = SizedMessage

    def parse_from_str(self, x):
        parts = x.split(">")
        return SizedMessage(parts[0], parts[1])

    def to_str(self, x):
        return x.msg + ">" + str(x.size)


# This registers value type with value type registry
register_value_type(MessageValueType())


# 1. create file extension
z_file_ext = register_file_extension("z")


class JoblibSizedMessageMarshaller(Marshaller):
    def target_to_value(self, target, **kwargs):
        with target.open() as fp:
            from_file = joblib.load(fp.name)
            return from_file

    def value_to_target(self, value, target, **kwargs):
        with target.open("w") as fp:
            joblib.dump(value, fp.name)


# 2. register type to extension mapping
register_marshaller(SizedMessage, z_file_ext, JoblibSizedMessageMarshaller())


@task(result=output.target_config(TargetConfig(format=z_file_ext)))
def dump_as_joblib():
    # type: ()-> SizedMessage
    return SizedMessage("example message \n", 10)


@task(result=output.txt[int])
def load_as_joblib(sized_message: SizedMessage):
    return sized_message.msg * sized_message.size
