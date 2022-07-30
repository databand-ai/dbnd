# © Copyright Databand.ai, an IBM Company 2022

import logging
import time

from dbnd._core.current import current_task_run
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.utils.basics.text_banner import TextBanner


logger = logging.getLogger(__name__)


class SageMakerCtrl(object):
    def __init__(self, task):
        self.task = task
        self.estimator = self.task.estimator_config.get_estimator(task)

    def train(self):
        self.estimator.fit(
            inputs=self.task.estimator_config.get_input_dict(
                train=self.task.train, test=self.task.train, validate=self.task.validate
            ),
            wait=False,
            logs=False,
        )
        self.wait_and_report_training_status()

    def _logs_url(self):
        region = self.estimator.sagemaker_session.boto_session.region_name
        return (
            "https://{0}.console.aws.amazon.com/cloudwatch/"
            "home?region={0}#logStream:group=/aws/sagemaker/TrainingJobs;streamFilter=typeLogStreamPrefix".format(
                region
            )
        )

    def _job_url(self, job_name):
        region = self.estimator.sagemaker_session.boto_session.region_name
        return "https://{0}.console.aws.amazon.com/sagemaker/home?{0}#/jobs/{1}".format(
            region, job_name
        )

    def wait_and_report_training_status(self):
        timeout = self.task.max_ingestion_time
        sec = 0
        client = self.estimator.sagemaker_session.sagemaker_client
        job = self.estimator.latest_training_job

        while True:
            description = client.describe_training_job(TrainingJobName=job.name)
            logger.info(self._get_job_status_banner(description))

            if description["TrainingJobStatus"] == "Completed":
                for metric in description["FinalMetricDataList"]:
                    self.task.log_metric(metric["MetricName"], metric["Value"])
                break

            if timeout and sec > timeout:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise DatabandRuntimeError(
                    "SageMaker job took more than %s seconds" % timeout
                )

            time.sleep(self.task.check_interval)
            sec = sec + self.task.check_interval

        status = description["TrainingJobStatus"]
        if status in "Failed":
            reason = description.get("FailureReason", "(No reason provided)")
            raise DatabandRuntimeError(
                "Error training %s: %s Reason: %s" % (job.name, status, reason)
            )

        billable_time = (
            description["TrainingEndTime"] - description["TrainingStartTime"]
        ) * description["ResourceConfig"]["InstanceCount"]

        self.task.log_metric("billable time", str(billable_time))

    def _get_job_status_banner(self, description):
        t = self.task
        b = TextBanner(
            "Training Job %s is running at SageMaker:"
            % description.get("TrainingJobName", None),
            color="yellow",
        )

        b.column("TASK", t.task_id)
        b.column(
            "JOB STATUS",
            description.get("TrainingJobStatus", None)
            + " -> "
            + description.get("SecondaryStatus", None),
        )
        b.column(
            "JOB RESOURCES",
            description["ResourceConfig"]["InstanceType"]
            + " x "
            + str(description["ResourceConfig"]["InstanceCount"]),
        )

        tracker_url = current_task_run().task_tracker_url
        if tracker_url:
            b.column("DATABAND LOG", tracker_url)
        b.column("JOB WEB UI", self._job_url(description.get("TrainingJobName", None)))
        b.column("CLOUDWATCH URL", self._logs_url())

        b.new_line()

        b.column("JOB ARN", description.get("TrainingJobArn", None))
        b.new_section()

        return b.getvalue()
