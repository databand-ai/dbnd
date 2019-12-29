import logging
import subprocess
import time

from dbnd._core.plugin.dbnd_plugins import assert_plugin_enabled
from dbnd._core.task_run.task_engine_ctrl import TaskEnginePolicyCtrl
from dbnd._core.utils.basics.text_banner import TextBanner
from dbnd._core.utils.structures import list_of_strings
from dbnd_qubole.errors import failed_to_run_qubole_job, failed_to_submit_qubole_job
from dbnd_spark.spark import SparkCtrl, SparkTask
from qds_sdk.commands import Command, SparkCommand
from qds_sdk.qubole import Qubole


logger = logging.getLogger(__name__)


def get_cloud_sync(config, task, job):
    assert_plugin_enabled("dbnd-aws", "qubole on aws requires dbnd-aws module.")


class QuboleCtrl(TaskEnginePolicyCtrl, SparkCtrl):
    def __init__(self, task_run):
        super(QuboleCtrl, self).__init__(task=task_run.task, job=task_run)
        self.qubole_config = task_run.task.spark_engine  # type: QuboleConfig
        Qubole.configure(
            api_token=self.qubole_config.api_token,
            api_url=self.qubole_config.api_url,
            cloud_name=self.qubole_config.cloud,
        )
        from dbnd_aws.aws_sync_ctrl import AwsSyncCtrl

        self.cloud_sync = AwsSyncCtrl(task_run.task, task_run)

    def _print_partial_log(self, cmd, err_ptr, log_ptr):
        log, err_len, log_len = cmd.get_log_partial(err_ptr, log_ptr)
        logger.debug(log, err_len, log_len)
        new_log_bytes = int(err_len) + int(log_len) - log_ptr
        if int(err_len) > 0:
            err_ptr += int(err_len)
            if new_log_bytes > 0:
                log = log[-new_log_bytes]
        else:
            log_ptr += int(log_len)
            err_ptr += int(err_len)
        return log, err_ptr, log_ptr, new_log_bytes

    def _handle_qubole_operator_execution(self, task_id):
        """
        Handles the Airflow + Databricks lifecycle logic for a Databricks operator
        :param run_id: Databricks run_id
        :param hook: Airflow databricks hook
        :param task_id: Databand Task Id.

        """
        b = TextBanner(
            "Spark task {} is submitted to Qubole cluster labeled: {}".format(
                task_id, self.qubole_config.cluster_label
            ),
            color="blue",
        )
        cmd_id = self.qubole_cmd_id
        host = self.qubole_config.api_url.replace("/api", "")
        url = "{host}/v2/analyze?command_id={id}".format(host=host, id=cmd_id)
        self.task_run.set_external_resource_urls({"qubole url": url})
        b.column("URL", url)
        logger.info(b.get_banner_str())
        log_ptr, err_ptr = 0, 0

        # CAN BREAK if QDS change their name of logger.
        qds_log_lvl = self.qubole_config.qds_sdk_logging_level
        logger.info("setting QDS logger level to: {}".format(qds_log_lvl))
        logger_qds = logging.getLogger("qds_connection")
        logger_qds.setLevel(qds_log_lvl)
        while True:
            cmd = SparkCommand.find(cmd_id)
            status = cmd.status
            b = TextBanner(
                "Spark task {} is submitted to Qubole cluster labeled: {}".format(
                    task_id, self.qubole_config.cluster_label
                ),
                color="blue",
            )
            b.column("Status", status)
            b.column("URL", url)
            logger.info(b.get_banner_str())

            log, err_ptr, log_ptr, received_log = self._print_partial_log(
                cmd, err_ptr, log_ptr
            )
            if received_log > 0 and self.qubole_config.show_spark_log:
                logger.info("Spark LOG:")
                logger.info(log)
            if SparkCommand.is_done(status):
                if SparkCommand.is_success(status):
                    b.column("Task completed successfully", task_id)
                    logger.info(b.get_banner_str())
                    return
                else:  # failed
                    cmd.get_results(fp=logger.error)
                    logger.info(b.get_banner_str())
                    raise failed_to_run_qubole_job(status, url, self.get_result())
            else:
                time.sleep(self.qubole_config.status_polling_interval_seconds)

    def run_pyspark(self, pyspark_script):
        # should be reimplemented using SparkSubmitHook (maybe from airflow)
        # note that config jars are not supported.

        pyspark_script = self.sync(pyspark_script)
        arguments = subprocess.list2cmdline(
            list_of_strings(self.task.application_args())
        )

        self.qubole_cmd_id = SparkCommand.create(
            script_location=pyspark_script,
            language="python",
            user_program_arguments=arguments,
            label=self.qubole_config.cluster_label,
            name=self.task.task_id,
        ).id
        self._handle_qubole_operator_execution(self.task.spark_engine.task_id)

        return True

    def get_logs(self):
        if self.qubole_cmd_id:
            return SparkCommand.get_log_id(self.qubole_cmd_id)
        return None

    def get_result(self):
        if self.qubole_cmd_id:
            return SparkCommand.find(self.qubole_cmd_id).get_results()

    # runs Java apps
    def run_spark(self, main_class):
        pass

    def on_kill(self):
        self.stop_spark_session(None)

    def stop_spark_session(self, session):
        try:
            SparkCommand.cancel(self.qubole_cmd_id)
        except Exception as e:
            logger.error("Failed to stop qubole", e)

    def sync(self, local_file):
        return self.cloud_sync.sync(local_file)
