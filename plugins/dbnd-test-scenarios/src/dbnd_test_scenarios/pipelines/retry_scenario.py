# © Copyright Databand.ai, an IBM Company 2022

"""
This is a retry scenario to test k8s execution with retries of a failed task.
CMD:
    `dbnd run dbnd_test_scenarios.pipelines.retry_scenario.retry_pipeline --task-version now --env=gcp_k8s --interactive`
    run with relevant config:
        `kubernetes.namespace` | `kubernetes.service_account_name` | `kubernetes.container_tag`
"""
import sys

from dbnd import current_task_run, pipeline, task


@task(task_retries=5)
def retry_task():
    task_run = current_task_run()
    if task_run.attempt_number < 5:
        sys.exit(130)


@pipeline
def retry_pipeline():
    retry_task()
