from dbnd import parameter
from dbnd._core.task import Config


class BaseMonitorConfig(Config):
    _conf__task_family = "source_monitor"

    prometheus_port = parameter(default=8000, description="Port for prometheus")[int]

    interval = parameter(
        default=5, description="Sleep time (in seconds) between fetches when not busy"
    )[int]

    runner_type = parameter(default="seq")[str]  # seq/mp

    number_of_iterations = parameter(
        default=None, description="Optional. Cap for the number of monitor iterations"
    )[int]

    stop_after = parameter(
        default=None,
        description="Optional. Cap for the number of seconds to run the monitor",
    )[int]
