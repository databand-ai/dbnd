import logging
import re

from dbnd._core.utils.better_subprocess import run_cmd
from dbnd_gcp.apache_beam.apache_beam_ctrl import ApacheBeamJobCtrl
from dbnd_gcp.dataflow.dataflow_config import DataflowConfig


logger = logging.getLogger(__name__)

_DATAFLOW_ID_REGEXP = re.compile(
    r".*console.cloud.google.com/dataflow.*/jobs/([a-z0-9A-Z\-_]+).*"
)


class DataFlowJobCtrl(ApacheBeamJobCtrl):
    def __init__(self, task_run):
        super(DataFlowJobCtrl, self).__init__(task_run=task_run)
        self.dataflow_config = task_run.task.beam_engine  # type: DataflowConfig

        gcp_conn_id = self.task_env.conn_id

        from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook

        self._gcp_dataflow_hook = DataFlowHook(
            gcp_conn_id=gcp_conn_id, delegate_to=self.task_env.delegate_to
        )
        if self.dataflow_config.temp_location:
            # override sync location with temp_location
            self.remote_sync_root = self.dataflow_config.temp_location

        self.current_dataflow_job_id = None

    def _get_base_options(self):
        options = super(DataFlowJobCtrl, self)._get_base_options()
        dfc = self.dataflow_config

        options.update(dfc.options)

        options.setdefault("runner", dfc.runner)
        options.setdefault("region", dfc.region)
        options.setdefault("project", dfc.project)
        options.setdefault("tempLocation", dfc.temp_location)

        return options

    def _process_dataflow_log(self, msg):
        msg = msg.strip()
        if self.current_dataflow_job_id is None:
            matched_job = _DATAFLOW_ID_REGEXP.search(msg)
            if matched_job:
                self.current_dataflow_job_id = matched_job.group(1)
                logger.info("Found dataflow job id '%s'", self.current_dataflow_job_id)
        logger.info(msg)

    def _run_cmd(self, cmd):
        dfc = self.dataflow_config

        from airflow.contrib.hooks.gcp_dataflow_hook import _DataflowJob

        run_cmd(
            cmd,
            name="dataflow %s" % self.task_run.job_name,
            stdout_handler=self._process_dataflow_log,
        )

        _DataflowJob(
            self._gcp_dataflow_hook.get_conn(),
            dfc.project,
            self.task_run.job_id,
            dfc.region,
            dfc.poll_sleep,
            self.current_dataflow_job_id,
        ).wait_for_done()
