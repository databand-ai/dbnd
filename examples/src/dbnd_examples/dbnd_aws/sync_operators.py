import fnmatch
import logging
import re

from airflow.hooks.S3_hook import S3Hook
from airflow.operators.sensors import BaseSensorOperator

from targets.utils.path import path_to_bucket_and_key


logger = logging.getLogger(__name__)

"""
This file contains Airflow operator and hook to monitor S3 path for new file.
Used by scheduled_s3_sensor DAG in  airflow/scheduled_sensor.py
"""


class S3SyncHook(S3Hook):
    def list_wildcard_keys(self, wildcard_key, bucket_name=None, delimiter=""):
        if not bucket_name:
            (bucket_name, wildcard_key) = self.parse_s3_url(wildcard_key)

        prefix = re.split(r"[*]", wildcard_key, 1)[0]
        klist = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        if klist:
            return [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
        return []

    def diff(self, source, destination):
        source_bucket, source_key = path_to_bucket_and_key(source)
        destination_bucket, destination_prefix = path_to_bucket_and_key(destination)
        destination_prefix += "/"

        source_state = self.list_wildcard_keys(source_key, source_bucket)
        destination_state = (
            self.list_keys(bucket_name=destination_bucket, prefix=destination_prefix)
            or []
        )
        logger.info("Full destination state: %s", destination_state)
        if destination_prefix:
            destination_state = [
                d[len(destination_prefix) :] for d in destination_state
            ]

        logging.info("source list:  {source_state}".format(**locals()))
        logging.info("destination list: {destination_state}".format(**locals()))

        diff = list(set(source_state) - set(destination_state))

        return ["s3://" + source_bucket + "/" + key for key in diff]


class S3StatefulSensor(BaseSensorOperator):
    from airflow.utils.decorators import apply_defaults

    template_fields = ("source", "destination")

    @apply_defaults
    def __init__(
        self, destination, source, aws_conn_id="aws_default", verify=None, **kwargs
    ):
        self.source = source
        self.destination = destination

        self.aws_conn_id = aws_conn_id
        self.verify = verify

        super(S3StatefulSensor, self).__init__(**kwargs)

    def poke(self, context):
        hook = S3SyncHook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        logging.info(
            "Poking for key : {self.source}, syncing with: {self.destination})".format(
                **locals()
            )
        )

        diff = hook.diff(self.source, self.destination)
        if diff:
            return True
        return False
