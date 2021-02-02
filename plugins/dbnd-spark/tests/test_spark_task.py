import os

import mock
import pytest

from dbnd import config, dbnd_config, parameter
from dbnd._core.errors import DatabandRunError
from dbnd.tasks import Config
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_airflow_contrib.mng_connections import set_connection
from dbnd_spark import SparkConfig
from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from targets import target
from tests.conftest import skip_require_java_build


class LocalSparkTestConfig(Config):
    spark_home = parameter()[str]
    host = parameter(default="local")
    conn_uri = parameter(
        default="docker://local?master=local&spark-home=%s" % spark_home,
        description="Airflow connection URI to use",
    )


@pytest.fixture()
def spark_config(databand_test_context):
    config = LocalSparkTestConfig()
    set_connection(
        conn_id="spark_default",
        host="local",
        conn_type="docker",
        extra='{"master":"local","spark-home": "%s"}' % config.spark_home,
    )

    return config


CONFIG_1 = "spark.sql.shuffle.partitions"
CONFIG_2 = "spark.executor.cores"


class TaskA(WordCountPySparkTask):
    _conf__tracked = False
    spark_conf_extension = {
        CONFIG_1: "TaskA",
    }


class TaskB(TaskA):
    spark_conf_extension = {
        CONFIG_2: "TaskB",
    }


class TaskC(WordCountPySparkTask):
    _conf__tracked = False
    spark_conf_extension = {
        CONFIG_1: "TaskC",
    }


class TaskD(TaskA):
    spark_conf_extension = {}


@pytest.mark.spark
class TestSparkTasksLocally(object):
    def spark_hook_params(self):
        return dict(
            application_args=mock.ANY,
            conn_id=mock.ANY,
            driver_class_path=mock.ANY,
            driver_memory=mock.ANY,
            env_vars=mock.ANY,
            exclude_packages=mock.ANY,
            executor_cores=mock.ANY,
            executor_memory=mock.ANY,
            files=mock.ANY,
            jars=mock.ANY,
            java_class=mock.ANY,
            keytab=mock.ANY,
            name=mock.ANY,
            num_executors=mock.ANY,
            packages=mock.ANY,
            principal=mock.ANY,
            py_files=mock.ANY,
            repositories=mock.ANY,
            total_executor_cores=mock.ANY,
            verbose=mock.ANY,
        )

    @pytest.fixture(autouse=True)
    def spark_conn(self, spark_config):
        self.config = spark_config

    # Requires the java example project to be built. To build java project you need Maven To use Maven you need
    # available docker (doesn't exist in this image) You can either create new docker image that has both spark and
    # maven, or create new docker image that has both spark and docker
    @skip_require_java_build
    def test_word_count_pyspark(self):
        actual = WordCountPySparkTask(text=__file__)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    @skip_require_java_build
    @mock.patch("dbnd_spark.local.local_spark.is_airflow_enabled", return_value=False)
    def test_word_count_pyspark_local_spark(self, _):
        actual = WordCountPySparkTask(text=__file__)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    @skip_require_java_build
    def test_word_spark(self):
        actual = WordCountTask(text=__file__)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark_with_error(self):
        actual = WordCountThatFails(text=__file__)
        with pytest.raises(DatabandRunError):
            actual.dbnd_run()

    def test_spark_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        # Solve "tests" module conflict on pickle loading after spark-submit
        parent_directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with dbnd_config({SparkConfig.env_vars: {"PYTHONPATH": parent_directory}}):
            assert_run_task(word_count_inline.t(text=__file__))

    def test_spark_inline_same_context(self):
        from pyspark.sql import SparkSession
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        with SparkSession.builder.getOrCreate() as sc:
            with config({SparkLocalEngineConfig.enable_spark_context_inplace: True}):
                assert_run_task(word_count_inline.t(text=__file__))

    def test_spark_inline_with_inplace_df(self):
        from pyspark.sql import SparkSession
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        with SparkSession.builder.getOrCreate() as sc:
            with config({SparkLocalEngineConfig.enable_spark_context_inplace: True}):
                inplace_df = sc.read.csv(__file__)
                assert_run_task(word_count_inline.t(text=inplace_df))

    @pytest.mark.skip("Broken because of user code in 'word_count_inline_folder'")
    def test_spark_io(self):
        from dbnd_test_scenarios.spark.spark_io_inline import dataframes_io_pandas_spark

        # Solve "tests" module conflict on pickle loading after spark-submit
        parent_directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        with dbnd_config({SparkConfig.env_vars: {"PYTHONPATH": parent_directory}}):
            assert_run_task(dataframes_io_pandas_spark.t(text=__file__))

    @pytest.mark.skip("Broken on missing imports")
    @mock.patch("dbnd_spark.local.local_spark.SparkSubmitHook")
    @mock.patch("dbnd_spark.spark._InlineSparkTask.current_task_run")
    @mock.patch("dbnd_spark.spark.get_databand_run")
    @mock.patch(
        "dbnd._core.task_run.task_run_logging.TaskRunLogManager.capture_task_log"
    )
    @mock.patch("dbnd._core.task_run.task_run.TaskRun.set_task_run_state")
    @mock.patch("dbnd._core.task_ctrl.task_ctrl.TaskCtrl.save_task_band")
    @mock.patch(
        "dbnd._core.task_ctrl.task_validator.TaskValidator.validate_task_is_complete"
    )
    @mock.patch("dbnd._core.run.databand_run.DatabandRun.save_run")
    def test_spark_hook(
        self, _, __, ___, ____, _____, ______, current_task_run, mock_hook
    ):
        _config = current_task_run.task.spark_config

        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        word_count_inline.t(text=__file__).dbnd_run()

        mock_hook.assert_called_once_with(
            application_args=[],
            conf=_config.conf,
            conn_id="spark_default",
            driver_class_path=_config.driver_class_path,
            driver_memory=_config.driver_memory,
            env_vars={
                "DBND_TASK_RUN_ATTEMPT_UID": str(current_task_run.task_run_attempt_uid)
            },
            exclude_packages=_config.exclude_packages,
            executor_cores=_config.executor_cores,
            executor_memory=_config.executor_memory,
            files="",
            jars=str(_config.main_jar),
            java_class=current_task_run.task.main_class,
            keytab=_config.keytab,
            name=current_task_run.job_id,
            num_executors=_config.num_executors,
            packages=_config.packages,
            principal=_config.principal,
            py_files="",
            repositories=_config.repositories,
            total_executor_cores=_config.total_executor_cores,
            verbose=_config.verbose,
        )

    @pytest.mark.parametrize(
        "task, expected",
        [
            (TaskA, {CONFIG_1: "TaskA", CONFIG_2: "config_layer"}),
            (TaskB, {CONFIG_1: "config_layer", CONFIG_2: "TaskB"}),
        ],
    )
    @mock.patch("airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook")
    @mock.patch("dbnd._core.settings.CoreConfig.build_tracking_store")
    @mock.patch(
        "dbnd._core.task_ctrl.task_validator.TaskValidator.validate_task_is_complete"
    )
    def test_spark_conf_merge(self, _, __, spark_submit_hook, task, expected):
        with dbnd_config(
            {
                SparkConfig.disable_sync: True,
                SparkConfig.disable_tracking_api: True,
                SparkConfig.conf: {CONFIG_1: "config_layer", CONFIG_2: "config_layer"},
            }
        ):
            task(text=__file__).dbnd_run()
            spark_submit_hook.assert_called_once_with(
                conf=expected, **self.spark_hook_params()
            )

    @mock.patch("airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook")
    @mock.patch("dbnd._core.settings.CoreConfig.build_tracking_store")
    @mock.patch(
        "dbnd._core.task_ctrl.task_validator.TaskValidator.validate_task_is_complete"
    )
    def test_spark_conf_merge_not_overlap(self, _, __, spark_submit_hook):
        with dbnd_config(
            {
                SparkConfig.disable_sync: True,
                SparkConfig.disable_tracking_api: True,
                SparkConfig.conf: {CONFIG_1: "config_layer", CONFIG_2: "config_layer"},
            }
        ):
            TaskA(text=__file__).dbnd_run()

            spark_submit_hook.assert_called_with(
                conf={CONFIG_1: "TaskA", CONFIG_2: "config_layer"},
                **self.spark_hook_params()
            )

            TaskB(text=__file__).dbnd_run()
            spark_submit_hook.assert_called_with(
                conf={CONFIG_1: "config_layer", CONFIG_2: "TaskB"},
                **self.spark_hook_params()
            )

            TaskC(text=__file__).dbnd_run()
            spark_submit_hook.assert_called_with(
                conf={CONFIG_1: "TaskC", CONFIG_2: "config_layer"},
                **self.spark_hook_params()
            )

            TaskD(text=__file__).dbnd_run()
            spark_submit_hook.assert_called_with(
                conf={CONFIG_1: "config_layer", CONFIG_2: "config_layer"},
                **self.spark_hook_params()
            )
