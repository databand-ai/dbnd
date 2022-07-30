# © Copyright Databand.ai, an IBM Company 2022

import json

from typing import List

import yaml

from six.moves import cPickle as pickle

from targets.marshalling.marshaller import Marshaller
from targets.target_config import FileFormat


class StrMarshaller(Marshaller):
    type = str
    file_format = FileFormat.txt

    clears_types_to_str = True

    def target_to_value(self, target, mode="r"):
        with target.open(mode) as fp:
            return fp.read()

    def value_to_target(self, value, target, mode="w"):
        if value is None:
            value = ""
        with target.open(mode) as fp:
            fp.write(value)
            fp.flush()


class StrLinesMarshaller(Marshaller):
    type = List[str]
    file_format = FileFormat.txt

    support_directory_direct_read = True
    support_multi_target_direct_read = True
    support_directory_direct_write = False
    clears_types_to_str = True

    def target_to_value(self, target, mode="r"):
        with target.open(mode) as fp:
            return fp.readlines()

    def value_to_target(self, value, target, mode="w", newline=True):
        if value is None:
            value = ""
        elif newline:
            value = ("%s\n" % s for s in value)
        with target.open(mode) as fp:
            fp.writelines(str(v) for v in value)
            fp.flush()
        return target


class ObjPickleMarshaller(Marshaller):
    type = object
    file_format = FileFormat.pickle

    def target_to_value(self, target, **kwargs):
        with target.open("rb") as fp:
            return pickle.load(fp, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        with target.open("wb") as fp:
            pickle.dump(value, fp, **kwargs)
        return target


class ObjJsonMarshaller(Marshaller):
    type = object
    file_format = FileFormat.json

    def target_to_value(self, target, **kwargs):
        with target.open("r") as fp:
            return json.load(fp=fp, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        from dbnd._core.utils.json_utils import json_default

        kwargs.setdefault("default", json_default)
        with target.open("w") as fp:
            json.dump(obj=value, fp=fp, **kwargs)
        return target


class ObjYamlMarshaller(Marshaller):
    type = object
    file_format = FileFormat.yaml

    def target_to_value(self, target, **kwargs):
        with target.open("r") as fp:
            return yaml.load(stream=fp, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        from dbnd._core.utils.json_utils import json_default

        kwargs.setdefault("default", json_default)
        with target.open("w") as fp:
            yaml.dump(data=value, stream=fp, **kwargs)
        return target
