import pytest

from dbnd import dbnd_config, output, pipeline, task
from dbnd_gcp.fs import build_gcs_client
from targets.fs import FileSystems, register_file_system


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
class TestGcsSync:
    def test_sync_execution(self):
        register_file_system(FileSystems.gcs, build_gcs_client)
        with dbnd_config({write.task.res: "gs://my/bucket/path"}):
            write_read.dbnd_run()
