from airflow.utils.state import State

from dbnd._core.constants import TaskRunState
from dbnd._core.utils.timezone import utcnow
from dbnd_airflow.airflow_extensions.dal import update_airflow_task_instance_in_db


class AirflowTaskInstanceRetryController(object):
    def __init__(self, task_instance, task_run):
        self.task_instance = task_instance
        self.task_run = task_run
        pass

    def schedule_task_instance_for_retry(
        self, retry_count, retry_delay, increment_try_number
    ):

        if self._is_eligible_to_retry(self.task_instance):
            self.task_run.task.task_retries = retry_count
            self.task_run.task.task_retry_delay = retry_delay
            self.task_instance.state = State.UP_FOR_RETRY
            # Ensure that end date has a value. If it does not - airflow crashes when calculating next retry datetime
            self.task_instance.end_date = self.task_instance.end_date or utcnow()
            if increment_try_number:
                self.task_instance._try_number += 1
            update_airflow_task_instance_in_db(self.task_instance)
            self.task_run.set_task_run_state(TaskRunState.UP_FOR_RETRY, track=True)
            return True
        else:
            return False

    @staticmethod
    def _is_eligible_to_retry(task_instance):
        """Is task instance is eligible for retry"""
        return task_instance.try_number <= task_instance.max_tries
