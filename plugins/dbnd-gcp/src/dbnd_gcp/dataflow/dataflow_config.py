# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict

from dbnd import parameter
from dbnd._core.settings.engine import EngineConfig
from targets import Target


DEFAULT_DATAFLOW_LOCATION = "us-central1"


class DataflowConfig(EngineConfig):
    """Google Dataflow"""

    _conf__task_family = "dataflow"

    project = parameter[str]
    region = parameter(default=DEFAULT_DATAFLOW_LOCATION)[str]
    temp_location = parameter(default=None).folder[Target]

    poll_sleep = parameter.value(5)

    options = parameter(empty_default=True)[Dict[str, str]]
    runner = parameter.value("DataflowRunner")

    def get_beam_ctrl(self, task_run):
        from dbnd_gcp.dataflow.dataflow import DataFlowJobCtrl

        return DataFlowJobCtrl(task_run)
