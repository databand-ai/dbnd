# © Copyright Databand.ai, an IBM Company 2022
from typing import Optional

import attr

from dbnd import parameter
from dbnd._core.task import Config


class BaseMonitorConfig(Config):
    _conf__task_family = "source_monitor"

    prometheus_port = parameter(
        default=8000, description="Set which port will be used for prometheus."
    )[int]

    interval = parameter(
        default=5,
        description="Set the sleep time, in seconds, between fetches, when the monitor is not busy.",
    )[int]

    runner_type = parameter(default="seq")[str]  # seq/mp

    number_of_iterations = parameter(
        default=None,
        description="Set a cap for the number of monitor iterations. This is optional.",
    )[int]

    stop_after = parameter(
        default=None,
        description="Set a cap for the number of seconds to run the monitor. This is optional.",
    )[int]

    log_format = parameter(default="text", description="Log format to use: text/json")[
        str
    ]

    @property
    def use_json_logging(self):
        return self.log_format == "json"


NOTHING = object()


@attr.s(auto_attribs=True)
class BaseMonitorState:
    monitor_error_message: Optional[str] = NOTHING

    def as_dict(self) -> dict:
        return {k: v for k, v in attr.asdict(self).items() if v is not NOTHING}
