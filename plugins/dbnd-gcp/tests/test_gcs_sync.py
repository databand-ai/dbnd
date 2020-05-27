import pytest

from dbnd import dbnd_config, output, pipeline, task
from targets import Target

from .test_gcs import _GCSBaseTestCase


def save_my_custom_object(path, data):
    with open(path, "w") as fd:
        fd.writelines(data)


def load_my_custom_object(path):
    with open(path, "r") as fd:
        return fd.readlines()


@task
def read(loc):
    # type: (Target) -> str
    data = load_my_custom_object(loc.path)
    return " ".join(data)


@task
def write(data, res=output.data.require_local_access):
    save_my_custom_object(res, data)


@pipeline
def write_read(data=["abcd", "zxcvb"]):
    path = write(data)
    r = read(path.res)
    return r


@pytest.mark.gcp
class TestGcsSync(_GCSBaseTestCase):
    def test_sync_execution(self):
        with dbnd_config({write.task.res: self.bucket_url("write_destination")}):
            write_read.dbnd_run()
