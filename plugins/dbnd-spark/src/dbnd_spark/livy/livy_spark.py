import logging

import six

from dbnd._core.current import current_task_run
from dbnd._core.utils.basics.text_banner import TextBanner
from dbnd._core.utils.http.endpoint import Endpoint
from dbnd._core.utils.structures import list_of_strings
from dbnd._vendor.termcolor import colored
from dbnd_spark.livy.livy_batch import LivyBatchClient
from dbnd_spark.livy.livy_spark_config import LivySparkConfig
from dbnd_spark.spark import SparkTask
from dbnd_spark.spark_ctrl import SparkCtrl


logger = logging.getLogger(__name__)


class _LivySparkCtrl(SparkCtrl):
    def get_livy_endpoint(self):
        raise NotImplementedError("This engine should implement get_livy_endpoint")

    def _run_spark_submit(self, file, jars):
        """
        Request Body	Description	Type
            file	File containing the application to run (required)	path
            proxyUser	User ID to impersonate when running the job	string
            className	Application Java or Spark main class	string
            args	Command line arguments for the application	list of strings
            jars	Jar files to be used in this session	list of strings
            pyFiles	Python files to be used in this session	list of strings
            files	Other files to be used in this session	list of strings
            driverMemory	Amount of memory to use for the driver process	string
            driverCores	Number of cores to use for the driver process	int
            executorMemory	Amount of memory to use for each executor process	string
            executorCores	Number of cores to use for each executor	int
            numExecutors	Number of executors to launch for this session	int
            archives	Archives to be used in this session	list of strings
            queue	The name of the YARN queue to which the job should be submitted	string
            name	Name of this session	string
            conf	Spark configuration properties	Map of key=val
        :param task:
        :return:
        """
        task = self.task  # type: SparkTask
        _config = task.spark_config  #

        deploy = self.deploy
        data = dict(
            conf=_config.conf,
            file=deploy.sync(file),
            className=task.main_class,
            name=self.job.job_id,
            args=list_of_strings(task.application_args()),
            files=deploy.sync_files(_config.files),
            pyFiles=deploy.sync_files(_config.py_files),
            jars=deploy.sync_files(jars),
            executorCores=_config.executor_cores,
            executorMemory=_config.executor_memory,
            driverMemory=_config.driver_memory,
            driverCores=_config.executor_cores,
            proxyUser=_config.proxy_user,
            queue=_config.queue,
            archives=_config.archives,
            numExecutors=_config.num_executors,
        )
        data = {k: v for k, v in six.iteritems(data) if v is not None}
        livy_endpoint = self.get_livy_endpoint()
        logger.info("Connecting to: %s", livy_endpoint)

        livy = LivyBatchClient.from_endpoint(livy_endpoint)
        batch = livy.post_batch(data)
        livy.track_batch_progress(
            batch["id"], status_reporter=self._report_livy_batch_status
        )

    def _report_livy_batch_status(self, batch_response):
        logger.info(self._get_batch_progresss_banner(batch_response))

    def _get_batch_progresss_banner(self, batch_response):
        """
        {
          'id': 6,
          'state': 'success',
          'appId': 'application_1534487568579_0008',
          'appInfo': {
            'driverLogUrl': None,
            'sparkUiUrl': 'http://ip-172-31-70-109.ec2.internal:20888/proxy/application_1534487568579_0008/'
          },
          'log': [
            '\nYARN Diagnostics: '
          ]
        }
        :param response:
        :return:
        """
        t = self.task
        b = TextBanner("Spark Task %s is running at Livy:" % t.task_id, color="yellow")

        b.column("TASK", t.task_id)
        b.column("JOB STATE", batch_response.get("state", None))

        tracker_url = current_task_run().task_tracker_url
        if tracker_url:
            b.column("DATABAND LOG", tracker_url)

        b.new_line()

        b.column("LIVY ID", batch_response.get("id", None))

        if "appId" in batch_response:
            b.column("APP ID", batch_response["appId"])

            app_info = batch_response["appInfo"]
            b.column("DRIVER LOG", app_info["driverLogUrl"])
            if "sparkUiUrl" in app_info:
                spark_url = app_info["sparkUiUrl"]
                b.column(
                    "SPARK", colored(spark_url, on_color="on_blue", attrs=["bold"])
                )
        b.new_section()

        return b.getvalue()


class LivySparkCtrl(_LivySparkCtrl):
    def run_pyspark(self, pyspark_script):
        jars = list(self.config.jars)
        if self.config.main_jar:
            jars += [self.config.main_jar]

        return self._run_spark_submit(file=pyspark_script, jars=jars)

    def run_spark(self, main_class):
        return self._run_spark_submit(file=self.config.main_jar, jars=self.config.jars)

    def get_livy_endpoint(self):
        livy_config = self.task_run.task.spark_engine  # type: LivySparkConfig
        return Endpoint(livy_config.url)
