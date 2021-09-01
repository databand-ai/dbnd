import logging
import os
import time

from airflow import AirflowException

from dbnd._core.current import current_task, current_task_run
from dbnd._core.plugin.dbnd_plugins import assert_plugin_enabled
from dbnd._core.utils.basics.text_banner import TextBanner
from dbnd._core.utils.structures import list_of_strings
from dbnd_databricks.databricks_config import (
    DatabricksAwsConfig,
    DatabricksAzureConfig,
    DatabricksCloud,
    DatabricksConfig,
)
from dbnd_databricks.errors import (
    failed_to_run_databricks_job,
    failed_to_submit_databricks_job,
)
from dbnd_spark.spark import SparkTask, _BaseSparkTask
from dbnd_spark.spark_ctrl import SparkCtrl


logger = logging.getLogger(__name__)


class DatabricksCtrl(SparkCtrl):
    def __init__(self, task_run):
        super(DatabricksCtrl, self).__init__(task_run=task_run)
        self.databricks_config = task_run.task.spark_engine  # type: DatabricksConfig

        self.local_dbfs_mount = None
        if self.databricks_config.cloud_type == DatabricksCloud.azure:
            assert_plugin_enabled(
                "dbnd-azure", "Databricks on azure requires dbnd-azure module."
            )

            self.local_dbfs_mount = DatabricksAzureConfig().local_dbfs_mount

        self.current_run_id = None
        self.hook = None

    def _dbfs_scheme_to_local(self, path):
        if self.databricks_config.cloud_type == DatabricksCloud.aws:
            return path
        elif self.databricks_config.cloud_type == DatabricksCloud.azure:
            return path.replace("dbfs://", "/dbfs")

    def _dbfs_scheme_to_mount(self, path):
        if self.databricks_config.cloud_type != DatabricksCloud.azure:
            return path

        from dbnd_azure.fs.azure_blob import AzureBlobStorageClient

        (
            storage_account,
            container_name,
            blob_name,
        ) = AzureBlobStorageClient._path_to_account_container_and_blob(path)
        return "dbfs://%s" % (os.path.join(self.local_dbfs_mount, blob_name))

    def _handle_databricks_operator_execution(self, run_id, hook, task_id):
        """
        Handles the Airflow + Databricks lifecycle logic for a Databricks operator
        :param run_id: Databricks run_id
        :param hook: Airflow databricks hook
        :param task_id: Databand Task Id.

        """
        b = TextBanner(
            "Spark task %s is submitted to Databricks cluster:" % task_id, color="cyan"
        )
        url = hook.get_run_page_url(run_id)
        self.task_run.set_external_resource_urls({"databricks url": url})
        b.column("URL", url)
        logger.info(b.get_banner_str())
        while True:
            b = TextBanner(
                "Spark task %s is submitted to Databricks cluster:" % task_id,
                color="cyan",
            )
            b.column("URL", url)
            run_state = hook.get_run_state(run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    b.column("Task completed successfully", task_id)
                    b.column("State:", run_state.life_cycle_state)
                    b.column("Message:", run_state.state_message)
                    break
                else:
                    b.column("State", run_state.result_state)
                    b.column("Error Message:", run_state.state_message)
                    logger.info(b.get_banner_str())
                    raise failed_to_run_databricks_job(
                        run_state.result_state, run_state.state_message, url
                    )
            else:
                b.column("State:", run_state.life_cycle_state)
                b.column("Message:", run_state.state_message)
                time.sleep(self.databricks_config.status_polling_interval_seconds)
            logger.info(b.get_banner_str())

    def _create_spark_submit_json(self, spark_submit_parameters):
        spark_config = self.task.spark_config
        new_cluster = {
            "num_workers": self.databricks_config.num_workers,
            "spark_version": self.databricks_config.spark_version,
            # spark_conf not supported, instead uses 'conf'
            "conf": self.databricks_config.spark_conf,
            "node_type_id": self.databricks_config.node_type_id,
            "init_scripts": self.databricks_config.init_scripts,
            "cluster_log_conf": self.databricks_config.cluster_log_conf,
            "spark_env_vars": self._get_env_vars(self.databricks_config.spark_env_vars),
            "py_files": self.deploy.arg_files(self.task.get_py_files()),
            "files": self.deploy.arg_files(spark_config.files),
            "jars": self.deploy.arg_files(spark_config.jars),
        }
        if self.databricks_config.cloud_type == DatabricksCloud.aws:
            attributes = DatabricksAwsConfig()
            new_cluster["aws_attributes"] = {
                "instance_profile_arn": attributes.aws_instance_profile_arn,
                "ebs_volume_type": attributes.aws_ebs_volume_type,
                "ebs_volume_count": attributes.aws_ebs_volume_count,
                "ebs_volume_size": attributes.aws_ebs_volume_size,
            }
        else:
            # need to see if there are any relevant setting for azure or other databricks envs.
            pass

        # since airflow connector for now() does not support spark_submit_task, it is implemented this way.
        return {
            "spark_submit_task": {"parameters": spark_submit_parameters},
            "new_cluster": new_cluster,
            "run_name": self.task.task_id,
        }

    def _create_pyspark_submit_json(self, python_file, parameters):
        spark_python_task_json = {"python_file": python_file, "parameters": parameters}
        # since airflow connector for now() does not support spark_submit_task, it is implemented this way.
        return {
            "spark_python_task": spark_python_task_json,
            "existing_cluster_id": self.databricks_config.cluster_id,
            "run_name": self.task.task_id,
        }

    def _run_spark_submit(self, databricks_json):
        task = self.task  # type: SparkTask
        _config = task.spark_engine

        from airflow.contrib.hooks.databricks_hook import DatabricksHook

        self.hook = DatabricksHook(
            _config.conn_id,
            _config.connection_retry_limit,
            retry_delay=_config.connection_retry_delay,
        )
        try:
            logging.debug("posted JSON:" + str(databricks_json))
            self.current_run_id = self.hook.submit_run(databricks_json)
            self.hook.log.setLevel(logging.WARNING)
            self._handle_databricks_operator_execution(
                self.current_run_id, self.hook, _config.task_id
            )
            self.hook.log.setLevel(logging.INFO)
        except AirflowException as e:
            raise failed_to_submit_databricks_job(e)

    def run_pyspark(self, pyspark_script):
        # should be reimplemented using SparkSubmitHook (maybe from airflow)
        # note that config jars are not supported.
        if not self.databricks_config.cluster_id:
            spark_submit_parameters = [self.sync(pyspark_script)] + (
                list_of_strings(self.task.application_args())
            )
            databricks_json = self._create_spark_submit_json(spark_submit_parameters)
        else:
            pyspark_script = self.sync(pyspark_script)
            parameters = [
                self._dbfs_scheme_to_local(e)
                for e in list_of_strings(self.task.application_args())
            ]
            databricks_json = self._create_pyspark_submit_json(
                python_file=pyspark_script, parameters=parameters
            )

        return self._run_spark_submit(databricks_json)

    def run_spark(self, main_class):
        jars_list = []
        jars = self.config.jars
        if jars:
            jars_list = ["--jars"] + jars
        # should be reimplemented using SparkSubmitHook (maybe from airflow)
        spark_submit_parameters = [
            "--class",
            main_class,
            self.sync(self.config.main_jar),
        ] + (list_of_strings(self.task.application_args()) + jars_list)
        databricks_json = self._create_spark_submit_json(spark_submit_parameters)
        return self._run_spark_submit(databricks_json)

    def _report_step_status(self, step):
        logger.info(self._get_step_banner(step))

    def _get_step_banner(self, step):
        """
        {
          'id': 6,
          'state': 'success',
        }
        """
        t = self.task
        b = TextBanner("Spark Task %s is running at Emr:" % t.task_id, color="yellow")

        b.column("TASK", t.task_id)
        b.column("EMR STEP STATE", step["Step"]["Status"]["State"])

        tracker_url = current_task_run().task_tracker_url
        if tracker_url:
            b.column("DATABAND LOG", tracker_url)

        b.new_line()
        b.column("EMR STEP ID", step["Step"]["Id"])
        b.new_section()
        return b.getvalue()

    def sync(self, local_file):
        synced = self.deploy.sync(local_file)
        if self.databricks_config.cloud_type == DatabricksCloud.azure:
            return self._dbfs_scheme_to_mount(synced)
        return synced

    def on_kill(self):
        if self.hook and self.current_run_id:
            self.hook.cancel_run(self.current_run_id)
