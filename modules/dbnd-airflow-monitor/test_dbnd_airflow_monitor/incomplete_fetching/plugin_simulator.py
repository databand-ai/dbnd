import json

import pendulum

from dbnd import relative_path


def get_json_from_file(file_path):
    file_path = relative_path(__file__, file_path)
    with open(file_path, "r") as f:
        content = f.read()
        return json.loads(content)


def pendulum_convert(date_str):
    return pendulum.parse(str(date_str).replace(" 00:00", "Z"))


class PluginSimulator(object):
    """
    Simulate response from plugin by using an output example json file with one full run (with multiple task instances)
    Creates multiple number of such runs as specified in the number_of_runs parameter and time diff between run as
    specified in the diff_between_dag_runs_in_minutes parameter.
    This class is used mainly for testing incomplete data type1 - incomplete task instances from completed dag runs
    but can be rewritten and expanded to support every case.
    """

    def __init__(
        self, result_example, number_of_runs, diff_between_dag_runs_in_minutes=10
    ):
        self._create_data(
            result_example, number_of_runs, diff_between_dag_runs_in_minutes
        )

    def _create_dag_run(self, example_dag_run, minutes_to_add):
        start_date = pendulum_convert(example_dag_run["start_date"])
        end_date = pendulum_convert(example_dag_run["end_date"])
        execution_date = pendulum_convert(example_dag_run["execution_date"])
        dagrun_id = int(example_dag_run["dagrun_id"])

        result_start_date = start_date.add(minutes=minutes_to_add)
        result_end_date = end_date.add(minutes=minutes_to_add)
        result_execution_date = execution_date.add(minutes=minutes_to_add)
        result_dagrun_id = dagrun_id + 1

        result_dag_run = example_dag_run.copy()
        result_dag_run["start_date"] = str(result_start_date)
        result_dag_run["end_date"] = str(result_end_date)
        result_dag_run["execution_date"] = str(result_execution_date)
        result_dag_run["dagrun_id"] = str(result_dagrun_id)

        return result_dag_run

    def _create_task_instance(self, example_task_instance, minutes_to_add):
        if example_task_instance["start_date"] is not None:
            start_date = pendulum_convert(example_task_instance["start_date"])
            result_start_date = start_date.add(minutes=minutes_to_add)
        else:
            result_start_date = None

        if example_task_instance["end_date"] is not None:
            end_date = pendulum_convert(example_task_instance["end_date"])
            result_end_date = end_date.add(minutes=minutes_to_add)
        else:
            result_end_date = None

        execution_date = pendulum_convert(example_task_instance["execution_date"])
        result_execution_date = execution_date.add(minutes=minutes_to_add)

        result_task_instance = example_task_instance.copy()
        result_task_instance["start_date"] = str(result_start_date)
        result_task_instance["end_date"] = str(result_end_date)
        result_task_instance["execution_date"] = str(result_execution_date)

        return result_task_instance

    def _create_data(
        self, result_example_file, number_of_runs, diff_between_dag_runs_in_minutes
    ):
        """
        Initialise example data - dag run and task instances
        """
        self._result_example = get_json_from_file(result_example_file)
        task_instances = self._result_example["task_instances"]

        self._dag_runs_list = []
        self._task_instances_dict = {}

        minutes_to_add = 0
        for i in range(number_of_runs):
            example_dag_run = self._result_example["dag_runs"][0]
            result_dag_run = self._create_dag_run(example_dag_run, minutes_to_add)
            self._dag_runs_list.append(result_dag_run)

            self._task_instances_dict[i] = []
            for task_instance in task_instances:
                result_task_instance = self._create_task_instance(
                    task_instance, minutes_to_add
                )
                self._task_instances_dict[i].append(result_task_instance)

            self._task_instances_dict[i].sort(key=lambda x: x["task_id"])
            minutes_to_add += diff_between_dag_runs_in_minutes

    def get_plugin_return(self, since, quantity, offset):
        """
        Given the previously initialised data, simulate plugin return for the given parameters
        """
        response = self._result_example.copy()
        index = len(self._dag_runs_list)

        # Find the first index whose dag run has end_date bigger than since
        for i in range(len(self._dag_runs_list)):
            if pendulum_convert(self._dag_runs_list[i]["end_date"]) > since:
                index = i
                break

        result_dag_runs = []
        result_task_instances = []

        # Add all the dag runs and task_instances to result
        total = 0
        while total < quantity and index < len(self._dag_runs_list):
            down_limit = offset
            upper_limit = offset + quantity - total
            new_task_instances = self._task_instances_dict[index][
                down_limit:upper_limit
            ]
            if new_task_instances:
                result_dag_runs.append(self._dag_runs_list[index])
                result_task_instances.extend(new_task_instances)
                total += len(new_task_instances)
            if offset != 0:
                offset -= len(self._task_instances_dict[index])
                if offset < 0:
                    offset = 0
            index += 1

        response["dag_runs"] = result_dag_runs
        response["task_instances"] = result_task_instances
        response["since"] = str(since)

        return response
