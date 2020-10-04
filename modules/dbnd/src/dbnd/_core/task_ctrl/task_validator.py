import logging
import time

from collections import defaultdict

from dbnd._core.errors import DatabandConfigError, friendly_error
from dbnd._core.task_ctrl.task_ctrl import TaskCtrl
from dbnd._core.utils import json_utils
from dbnd._core.utils.traversing import flatten


# Time to sleep while waiting for eventual consistency to finish.
EVENTUAL_CONSISTENCY_SLEEP_INTERVAL = 1

# Maximum number of sleeps for eventual consistency.
EVENTUAL_CONSISTENCY_MAX_SLEEPS = 30


class TaskValidator(TaskCtrl):
    def __init__(self, task):
        super(TaskCtrl, self).__init__(task)

    def find_and_raise_missing_inputs(self):
        missing = find_non_completed(self.relations.task_outputs_user)
        missing_str = non_completed_outputs_to_str(missing)
        raise DatabandConfigError(
            "You are missing some input tasks in your pipeline! \n\t%s\n"
            "The task execution was disabled for '%s'."
            % (missing_str, self.task.task_id)
        )

    def validate_task_inputs(self):
        if not self.task.ctrl.should_run():
            missing = find_non_completed(self.relations.task_outputs_user)
            missing_str = non_completed_outputs_to_str(missing)
            raise DatabandConfigError(
                "You are missing some input tasks in your pipeline! \n\t%s\n"
                "The task execution was disabled for '%s'."
                % (missing_str, self.task.task_id)
            )

        missing = []
        for partial_output in flatten(self.relations.task_inputs_user):
            if not partial_output.exists():
                missing.append(partial_output)
        if missing:
            raise friendly_error.task_data_source_not_exists(
                self, missing, downstream=[self.task]
            )

    def validate_task_is_complete(self):
        if self.task._complete():
            return

        if self.wait_for_consistency():
            return

        missing = find_non_completed(self.relations.task_outputs_user)
        if not missing:
            raise friendly_error.task_has_not_complete_but_all_outputs_exists(self)

        missing_str = non_completed_outputs_to_str(missing)
        raise friendly_error.task_has_missing_outputs_after_execution(
            self.task, missing_str
        )

    def wait_for_consistency(self):
        for attempt in range(EVENTUAL_CONSISTENCY_MAX_SLEEPS):
            missing = find_non_completed(self.task.task_outputs)
            if not missing:
                return True
            missing_and_not_consistent = find_non_consistent(missing)
            if not missing_and_not_consistent:
                return False

            missing_str = non_completed_outputs_to_str(missing_and_not_consistent)
            logging.warning(
                "Some outputs are missing, potentially due to eventual consistency of "
                "your data store. Waining %s second to retry. Additional attempts: %s\n\t%s"
                % (
                    str(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL),
                    str(EVENTUAL_CONSISTENCY_MAX_SLEEPS - attempt),
                    missing_str,
                )
            )

            time.sleep(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL)
        return False


def find_non_completed(targets):
    missing = defaultdict(list)
    for k, v in targets.items():
        for partial_output in flatten(v):
            if not partial_output.exists():
                missing[k].append(partial_output)

    return missing


def find_non_consistent(targets):
    non_consistent = list()
    for k, v in targets.items():
        for partial_output in flatten(v):
            if not partial_output.exist_after_write_consistent():
                non_consistent.append(partial_output)

    return non_consistent


def non_completed_outputs_to_str(non_completed_outputs):
    return json_utils.dumps(non_completed_outputs)
