import os
import re

from io import StringIO
from logging import StreamHandler

from dbnd._core.errors.friendly_error.task_execution import (
    failed_spark_status,
    failed_to_run_spark_script,
)
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.utils.structures import list_of_strings
from dbnd_spark._core.spark_error_parser import parse_spark_log_safe
from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig
from dbnd_spark.spark_ctrl import SparkCtrl


class LocalSparkExecutionCtrl(SparkCtrl):
    def run_pyspark(self, pyspark_script):
        jars = list(self.config.jars)
        application = pyspark_script
        if self.config.main_jar:
            jars += [self.config.main_jar]

        return self._run_spark_submit(application=application, jars=jars)

    def run_spark(self, main_class):
        application = self.config.main_jar
        return self._run_spark_submit(application=application, jars=self.config.jars)

    def _run_spark_submit(self, application, jars):
        # task_env = get_cloud_config(Clouds.local)
        spark_local_config = SparkLocalEngineConfig()
        _config = self.config
        deploy = self.deploy

        AIRFLOW_ON = is_airflow_enabled()

        if AIRFLOW_ON:
            from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
            from airflow.exceptions import AirflowException as SparkException
        else:
            from dbnd_spark._vendor.airflow.spark_hook import (
                SparkException,
                SparkSubmitHook,
            )

        spark = SparkSubmitHook(
            conf=_config.conf,
            conn_id=spark_local_config.conn_id,
            name=self.job.job_id,
            application_args=list_of_strings(self.task.application_args()),
            java_class=self.task.main_class,
            files=deploy.arg_files(_config.files),
            py_files=deploy.arg_files(self.task.get_py_files()),
            driver_class_path=_config.driver_class_path,
            jars=deploy.arg_files(jars),
            packages=_config.packages,
            exclude_packages=_config.exclude_packages,
            repositories=_config.repositories,
            total_executor_cores=_config.total_executor_cores,
            executor_cores=_config.executor_cores,
            executor_memory=_config.executor_memory,
            driver_memory=_config.driver_memory,
            keytab=_config.keytab,
            principal=_config.principal,
            num_executors=_config.num_executors,
            env_vars=self._get_env_vars(),
            verbose=_config.verbose,
        )
        if not AIRFLOW_ON:
            # If there's no Airflow then there's no Connection so we
            # take conn information from spark config
            spark.set_connection(spark_local_config.conn_uri)

        log_buffer = StringIO()
        with log_buffer as lb:
            dbnd_log_handler = self._capture_submit_log(spark, lb)
            try:
                spark.submit(application=application)
            except SparkException as ex:
                return_code = self._get_spark_return_code_from_exception(ex)
                if return_code != "0":
                    error_snippets = parse_spark_log_safe(
                        log_buffer.getvalue().split(os.linesep)
                    )
                    raise failed_to_run_spark_script(
                        self,
                        spark._build_spark_submit_command(application=application),
                        application,
                        return_code,
                        error_snippets,
                    )
                else:
                    raise failed_spark_status(ex)
            finally:
                spark.log.handlers = [
                    h for h in spark.log.handlers if not dbnd_log_handler
                ]

    def _capture_submit_log(self, spark, log_buffer):
        ch = StreamHandler(log_buffer)
        spark.log.addHandler(ch)
        return ch

    def _get_spark_return_code_from_exception(self, exception):
        match = re.search("Error code is: ([0-9+]).", str(exception))
        if match:
            return match.groups()[0]
        # assume spark_submit success otherwise
        return "0"
