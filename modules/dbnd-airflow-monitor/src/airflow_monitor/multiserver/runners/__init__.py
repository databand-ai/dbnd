from airflow_monitor.multiserver.runners.multi_process_runner import MultiProcessRunner
from airflow_monitor.multiserver.runners.sequential_runner import SequentialRunner


RUNNER_FACTORY = {
    "seq": SequentialRunner,
    "mp": MultiProcessRunner,
}
