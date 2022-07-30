# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.current import current_task_run
from dbnd._core.utils.basics.text_banner import TextBanner
from dbnd._core.utils.structures import list_of_strings
from dbnd_aws.emr.emr_ctrl import EmrCtrl


class EmrStepCtrl(EmrCtrl):
    def _run_spark_submit(self, file, jars):
        from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook

        _config = self.config
        deploy = self.deploy
        spark = SparkSubmitHook(
            conf=_config.conf,
            conn_id=self.emr_config.conn_id,
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
            status_poll_interval=_config.status_poll_interval,
        )

        step_id = self.emr_cluster.run_spark_submit_step(
            name=self.job.job_id,
            spark_submit_command=spark._build_spark_submit_command(
                application=deploy.sync(file)
            ),
            action_on_failure=self.emr_config.action_on_failure,
        )
        self.task_run.set_external_resource_urls(
            self.emr_cluster.get_emr_logs_dict(self.spark_application_logs)
        )
        self.emr_cluster.wait_for_step_completion(
            step_id,
            status_reporter=self._report_step_status,
            emr_completion_poll_interval=self.emr_config.emr_completion_poll_interval,
        )

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
