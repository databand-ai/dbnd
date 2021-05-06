import glob

import pytest

from pytest import fixture

import sh


@fixture(autouse=True, scope="module")
def setup_spark_connection():
    cmd = sh.Command("airflow")
    cmd("connections", "--delete", "--conn_id", "spark_default")
    cmd(
        "connections",
        "--add",
        "--conn_id",
        "spark_default",
        "--conn_type",
        "docker",
        "--conn_host",
        "local",
        "--conn_extra",
        '\'{"master":"local"}\'',
        _truncate_exc=False,
    )
    sh.rm("-rf", "counters.csv", "outputs", "data", _truncate_exc=False)


def dbnd_run_cmd(task_name):
    # Disable web tracker because we don't use webserver in this test
    return sh.dbnd("run", task_name, "--disable-web-tracker", _truncate_exc=False)


class TestSparkRun(object):
    def test_spark_inline(self):
        output = dbnd_run_cmd("word_count_inline.word_count_inline").stderr.decode()
        assert "stage-2.runJob.internal.metrics.resultSize=1952" in output, output
        assert "stage-6.runJob.internal.metrics.resultSize=1622" in output, output
        assert (
            "stage-6.runJob.internal.metrics.shuffle.read.localBytesRead=111" in output
        ), output
        assert "Your run has been successfully executed!" in output, output

    def test_spark_task_class(self):
        output = dbnd_run_cmd("word_count.WordCountTask").stderr.decode()
        assert "Your run has been successfully executed!" in output, output

    def test_pyspark_task_class(self):
        output = dbnd_run_cmd("word_count.WordCountPySparkTask").stderr.decode()
        assert "Your run has been successfully executed!" in output, output

    @pytest.mark.skip("Test requires different inttest environment")
    def test_pyspark_plain_run(self):
        cmd = sh.Command("spark-submit")
        result = cmd(
            "word_count_plain.py", "word_count.py", "outputs", _truncate_exc=False
        )
        stdout = result.stdout.decode()
        stderr = result.stderr.decode()
        assert "Your run has been successfully executed!" in stdout, stdout
        assert "Running Spark" in stderr, stderr
        files = [
            x
            for x in glob.glob(
                "data/dbnd/log/*/*word_count_plain*/**/1-spark.log", recursive=True,
            )
        ]
        assert len(files) == 3
