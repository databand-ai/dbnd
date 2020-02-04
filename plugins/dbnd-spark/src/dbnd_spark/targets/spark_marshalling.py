from __future__ import absolute_import

import logging

from urllib.parse import urlparse

import pyspark.sql as spark

from dbnd._core.commands import get_spark_session
from targets.marshalling.marshaller import Marshaller
from targets.target_config import FileFormat
from targets.utils.performance import target_timeit


logger = logging.getLogger(__name__)


class SparkMarshaller(Marshaller):
    type = spark.DataFrame
    support_directory_direct_read = True
    support_multi_target_direct_read = True
    support_directory_direct_write = True

    def __init__(self, fmt=FileFormat.csv):
        self.file_format = fmt
        if self.file_format == FileFormat.txt:
            self.file_format = "text"

    @target_timeit
    def target_to_value(self, target, **kwargs):
        path = _target_to_path(target)
        schema = kwargs["schema"] if "schema" in kwargs else None
        return (
            get_spark_session()
            .read.format(self.file_format)
            .options(**kwargs)
            .load(path, schema=schema)
        )

    def value_to_target(self, value, target, **kwargs):
        path = _target_to_path(target)
        value.write.options(**kwargs).save(path=path, format=self.file_format)


class SparkDataFrameToCsv(SparkMarshaller):
    infer_schema = True
    header = True

    @target_timeit
    def target_to_value(self, target, **kwargs):
        # keep it backward compatible
        if "header" not in kwargs:
            kwargs["header"] = self.header
        if "inferSchema" not in kwargs:
            kwargs["inferSchema"] = self.infer_schema
        return super(SparkDataFrameToCsv, self).target_to_value(target, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        # keep it backward compatible
        if "header" not in kwargs:
            kwargs["header"] = self.header
        return super(SparkDataFrameToCsv, self).value_to_target(value, target, **kwargs)


def _target_to_path(target):
    from targets.multi_target import MultiTarget

    if isinstance(target, MultiTarget):
        return [
            _convert_http_to_wasb_for_azure(p)
            for p in _multi_target_to_path_list(target)
        ]

    return _convert_http_to_wasb_for_azure(target.path)


def _multi_target_to_path_list(target):
    return [partition.path for partition in target.targets]


def _convert_http_to_wasb_for_azure(address):
    scheme, netloc, path, _, _, _ = urlparse(address)

    if scheme not in ["http", "https"]:
        return address

    scheme = scheme.replace("http", "wasb")
    path = path.split("/")

    container = path[1]
    path = "/".join(path[2:])

    return "%s://%s@%s/%s" % (scheme, container, netloc, path)
