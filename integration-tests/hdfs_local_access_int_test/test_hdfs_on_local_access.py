import logging
import os
import subprocess
import time

import pandas as pd
import pytest

from dbnd import output, parameter, pipeline, task
from dbnd_hdfs.fs.hdfs_hdfscli import HdfsCli
from dbnd_hdfs.fs.hdfs_pyox import HdfsPyox


logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_HDFS_PATH = "hdfs://default/location/for_output.csv"
DEFAULT_SECOND_OUTPUT_HDFS_PATH = "hdfs://default/location/for_second_output.csv"


def pandas_data_frame():
    names = ["Bob", "Jessica", "Mary", "John", "Mel"]
    births = [968, 155, 77, 578, 973]
    df = pd.DataFrame(data=list(zip(names, births)), columns=["Names", "Births"])
    return df


def cmd_output(cmd):
    try:
        output = (
            subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, shell=True, env=os.environ
            )
            .decode("utf-8")
            .strip()
        )
    except subprocess.CalledProcessError as ex:
        logger.error(
            "Failed to run %s :\n\n\n -= Output =-\n%s\n\n\n -= See output above =-",
            cmd,
            ex.output.decode("utf-8", errors="ignore"),
        )
        raise ex
    return output


@task(result=output(default=DEFAULT_OUTPUT_HDFS_PATH).overwrite.csv[pd.DataFrame])
def overwrite_target_task(df=parameter[pd.DataFrame]):
    return df


@pipeline
def overwrite_target_pipeline():
    result = overwrite_target_task(pandas_data_frame())
    return result


class TestOverwritingTargetHdfs(object):
    @pytest.fixture(autouse=True, scope="module")
    def wait_for_namenode(self):
        # Wait for namenode to leave safe mode
        time.sleep(10)

    def test_execution_overwrite_target(self):
        import os

        client = HdfsPyox()
        client.put(
            os.path.abspath("for_output.csv"), DEFAULT_OUTPUT_HDFS_PATH, overwrite=True
        )

        # Pipeline will fail if file isn't overwritten
        overwrite_target_pipeline.dbnd_run(task_version="now")


class TestRequireLocalAccessOnHdfs(object):
    hdfs_file = "hdfs://my_bucket/my_path"
    hdfs_dir = "hdfs://my_dir_bucket/my_dir/"

    @pytest.fixture(autouse=True)
    def wait_for_namenode(self):
        # Wait for namenode to leave safe mode
        time.sleep(10)

    @pytest.fixture(autouse=True)
    def clean_env(self):
        hdfs_client = HdfsCli()
        hdfs_client.remove(TestRequireLocalAccessOnHdfs.hdfs_file)
        hdfs_client.remove(TestRequireLocalAccessOnHdfs.hdfs_dir)

    def test_execution_file_target(self):
        output = cmd_output(
            "dbnd run dbnd_test_scenarios.pipelines.simple_read_write_pipeline.write_read --set write.res={0} "
            "--task-version now".format(TestRequireLocalAccessOnHdfs.hdfs_file)
        )
        assert "Your run has been successfully executed!" in output, output
        print(output)

    def test_execution_dir_target(self):
        output = cmd_output(
            "dbnd run dbnd_test_scenarios.pipelines.simple_read_write_pipeline.write_read_dir --set write_dir.res={0} --task-version now".format(
                TestRequireLocalAccessOnHdfs.hdfs_dir
            )
        )
        assert "Your run has been successfully executed!" in output, output
        print(output)
