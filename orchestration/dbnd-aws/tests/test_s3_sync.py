# © Copyright Databand.ai, an IBM Company 2022

from dbnd import dbnd_config
from dbnd.orchestration.orchestration_bootstrap import dbnd_bootstrap_orchestration
from dbnd_test_scenarios.pipelines.simple_read_write_pipeline import (
    write,
    write_dir,
    write_read,
    write_read_dir,
)

from .test_s3 import _S3BaseTestCase


class TestS3Sync(_S3BaseTestCase):
    def test_sync_execution_file_target(self):
        dbnd_bootstrap_orchestration()
        self.client.remove(self.bucket_url("write_destination"))

        with dbnd_config({write.task.res: self.bucket_url("write_destination")}):
            write_read.dbnd_run()

    def test_sync_execution_dir_target(self):
        dbnd_bootstrap_orchestration()
        self.client.remove(self.bucket_url("write_destination/"))

        with dbnd_config({write_dir.task.res: self.bucket_url("write_destination/")}):
            write_read_dir.dbnd_run()
