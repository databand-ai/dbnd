import shlex

from typing import List

import six

from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.task_run.task_sync_ctrl import DisabledTaskSyncCtrl, TaskSyncCtrl
from dbnd._core.utils.basics.cmd_line_builder import CmdLineBuilder
from dbnd_spark import SparkConfig


class SparkCtrl(TaskRunCtrl):
    def __init__(self, task_run):
        super(SparkCtrl, self).__init__(task_run=task_run)

        if not self.config.disable_sync:
            self.deploy = self._get_deploy_ctrl()
        else:
            self.deploy = DisabledTaskSyncCtrl(task_run=task_run)

    def _get_deploy_ctrl(self):
        return self.task_run.deploy  # type: TaskSyncCtrl

    def should_keep_local_pickle(self):
        return False

    def download_to_local(self):
        run = self.job.run
        run.driver_dump.fs.download(
            run.driver_dump.path, run.driver_task.local_driver_dump
        )

    def spark_driver_dump_file(self):
        dr = self.job.run.driver_task
        return (
            dr.local_driver_dump if self.should_keep_local_pickle() else dr.driver_dump
        )

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

    def stop_spark_session(self, session):
        session.stop()

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
