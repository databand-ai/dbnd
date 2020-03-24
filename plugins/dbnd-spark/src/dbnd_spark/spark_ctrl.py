import shlex

from typing import List

import six

from dbnd import current
from dbnd._core.configuration.environ_config import DBND_TASK_RUN_ATTEMPT_UID
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.task_run.task_sync_ctrl import DisabledTaskSyncCtrl, TaskSyncCtrl
from dbnd._core.utils.basics.cmd_line_builder import CmdLineBuilder
from dbnd_spark import SparkConfig


class SparkCtrl(TaskRunCtrl):
    stop_spark_session_on_finish = False

    def __init__(self, task_run):
        super(SparkCtrl, self).__init__(task_run=task_run)

        if not self.config.disable_sync:
            self.deploy = self._get_deploy_ctrl()
        else:
            self.deploy = DisabledTaskSyncCtrl(task_run=task_run)

    def _get_deploy_ctrl(self):
        return self.task_run.deploy  # type: TaskSyncCtrl

    @property
    def config(self):
        # type: (SparkCtrl) -> SparkConfig
        return self.task.spark_config

    def run_pyspark(self, pyspark_script):
        raise NotImplementedError("This engine doesn't support pyspark jobs")

    def run_spark(self, main_class):
        raise NotImplementedError("This engine doesn't support spark jobs")

    # note that second variable should be changed on subclasses.
    spark_application_logs = {
        "YARN ResourceManager": ["http://", "<master>", ":8088"],
        "YARN NodeManager": ["http://", "<core>", ":8088"],
        "Hadoop HDFS NameNode": ["http://", "<master>", ":50070"],
        "Spark HistoryServer": ["http://", "<master>", ":18080"],
        "Ganglia": ["http://", "<master>", "/ganglia"],
    }

    def config_to_command_line(self):
        # type: ()-> List[str]
        config = self.config
        deploy = self.deploy
        cmd = CmdLineBuilder()
        if config.conf:
            for key, value in six.iteritems(config.conf):
                cmd.option("--conf", "{}={}".format(str(key), str(value)))

        cmd.option("--files", deploy.arg_files(config.files))
        cmd.option("--py-files", deploy.arg_files(config.py_files))
        cmd.option("--archives", deploy.arg_files(config.archives))
        cmd.option("--jars", deploy.arg_files(config.jars))

        if config.driver_class_path:
            cmd += ["--driver-class-path", config.driver_class_path]

        cmd.option("--packages", config.packages)
        cmd.option("--exclude-packages", config.exclude_packages)
        cmd.option("--repositories", config.repositories)
        cmd.option("--num-executors", config.num_executors)
        cmd.option("--total-executor-cores", config.total_executor_cores)
        cmd.option("--executor-cores", config.executor_cores)
        cmd.option("--executor-memory", config.executor_memory)
        cmd.option("--driver-memory", config.driver_memory)

        cmd.option("--keytab", config.keytab)
        cmd.option("--principal", config.principal)
        cmd.option("--proxy-user", config.proxy_user)
        cmd.option("--queue", config.queue)
        cmd.option("--deploy-mode", config.deploy_mode)
        cmd.option_bool("--verbose", config.verbose)

        if config.submit_args:
            cmd.add(*shlex.split(config.submit_args))
        return cmd.get_cmd()

    def sync(self, local_file):
        return self.deploy.sync(local_file)

    def _get_env_vars(self, conf_env_vars=None):
        env_vars = {
            DBND_TASK_RUN_ATTEMPT_UID: str(
                current().current_task_run.task_run_attempt_uid
            )
        }
        if conf_env_vars is None:
            conf_env_vars = self.config.env_vars
        if conf_env_vars:
            env_vars.update(conf_env_vars)
        return env_vars
