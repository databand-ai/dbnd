from time import sleep

from dbnd import pipeline, task
from dbnd._core.constants import TaskRunState


@task
def successful_task(param):
    print(param)
    return "success"


@task
def failing_task(input=None):
    raise IOError


@pipeline
def failing_pipeline():
    return failing_task()


@pipeline
def successful_pipeline(param=None):
    return successful_task("test123")


def task_state_validation_helper(run, expected_result_map):
    task_run_ids_to_inspect = [run.task.ctrl.task_id]

    # in order traversal on the task tree,starting at root each task test for the expected result
    while len(task_run_ids_to_inspect) > 0:
        current_task_run_id = task_run_ids_to_inspect.pop()
        task_run_instance = run.task_runs_by_id.get(current_task_run_id)
        if task_run_instance:
            assert task_run_instance.task_run_state == expected_result_map.get(
                task_run_instance.task.friendly_task_name
            )
            # Add children nodes to inspect in next iterations
            task_run_ids_to_inspect.extend(task_run_instance.task.descendants.children)


class TestSubPipelineExecution(object):
    def test_pipeline_with_failing_sub_pipeline(self):
        @pipeline
        def root_pipeline_with_failing_sub_pipeline():
            return successful_task(failing_pipeline())

        expected_result_map = {
            root_pipeline_with_failing_sub_pipeline.__name__: TaskRunState.FAILED,
            failing_pipeline.__name__: TaskRunState.FAILED,
            failing_task.__name__: TaskRunState.FAILED,
            successful_task.__name__: TaskRunState.UPSTREAM_FAILED,
        }
        try:
            root_pipeline_with_failing_sub_pipeline.dbnd_run()
        except Exception as e:
            run = e.run
            task_state_validation_helper(run, expected_result_map)

    def test_successful_sub_pipeline(self):
        @pipeline
        def root_pipeline_with_successful_sub_pipeline():
            return successful_task(successful_pipeline())

        expected_result_map = {
            root_pipeline_with_successful_sub_pipeline.__name__: TaskRunState.SUCCESS,
            successful_task.__name__: TaskRunState.SUCCESS,
        }
        try:
            root_pipeline_with_successful_sub_pipeline.dbnd_run()
        except Exception as e:
            run = e.run
            task_state_validation_helper(run, expected_result_map)

    def test_with_upstream_failed_sub_pipeline(self):
        @task
        def upstream_failed_task(sleeping_time):
            sleep(sleeping_time)
            return "15"

        @task
        def task_with_failing_sub_pipeline():
            failing_pipeline()
            sleep(2)
            return True

        @pipeline
        def root_pipeline_with_upstream_failed_pipeline():
            return upstream_failed_task(task_with_failing_sub_pipeline())

        expected_result_map = {
            root_pipeline_with_upstream_failed_pipeline.__name__: TaskRunState.FAILED,
            task_with_failing_sub_pipeline.__name__: TaskRunState.FAILED,
            failing_pipeline.__name__: TaskRunState.FAILED,
            failing_task.__name__: TaskRunState.FAILED,
            upstream_failed_task.__name__: TaskRunState.UPSTREAM_FAILED,
        }
        try:
            root_pipeline_with_upstream_failed_pipeline.dbnd_run()
        except Exception as e:
            run = e.run
            task_state_validation_helper(run, expected_result_map)
