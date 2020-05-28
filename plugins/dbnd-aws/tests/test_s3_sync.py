from dbnd import dbnd_config
from dbnd.tasks.basics.simple_read_write_pipeline import write, write_read

from .test_s3 import _S3BaseTestCase


class TestS3Sync(_S3BaseTestCase):
    def test_sync_execution(self):
        with dbnd_config({write.task.res: self.bucket_url("write_destination")}):
            write_read.dbnd_run()
