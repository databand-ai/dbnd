from __future__ import absolute_import

import logging

import pyspark.sql as spark

from dbnd_spark.spark_config import SparkMarshallingConfig
from dbnd_spark.spark_session import get_spark_session
from targets.marshalling.marshaller import Marshaller
from targets.target_config import FileFormat
from targets.utils.performance import target_timeit


try:
    import urlparse
except ImportError:
    from urllib.parse import urlparse


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

    def support_direct_access(self, target):
        # Spark supports direct access to all file systems
        return True


class SparkDataFrameToCsv(SparkMarshaller):
    @target_timeit
    def target_to_value(self, target, **kwargs):
        marshalling_config = SparkMarshallingConfig()
        # keep it backward compatible
        if "header" not in kwargs:
            default_header_value = marshalling_config.default_header_value
            kwargs["header"] = default_header_value
            logger.warning("Header value is %s", default_header_value)
        if "inferSchema" not in kwargs:
            default_infer_schema_value = marshalling_config.default_infer_schema_value
            kwargs["inferSchema"] = default_infer_schema_value
            logger.warning("Infer schema value is %s", default_infer_schema_value)
        return super(SparkDataFrameToCsv, self).target_to_value(target, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        marshalling_config = SparkMarshallingConfig()
        # keep it backward compatible
        if "header" not in kwargs:
            default_header_value = marshalling_config.default_header_value
            kwargs["header"] = default_header_value
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
