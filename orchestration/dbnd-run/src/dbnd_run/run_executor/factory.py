# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from typing import List

from dbnd._core.constants import TaskExecutorType
from dbnd._core.context.use_dbnd_run import is_dbnd_orchestration_via_airflow_enabled
from dbnd._core.errors import DatabandConfigError
from dbnd_run import errors
from dbnd_run.plugin.dbnd_plugins import is_plugin_enabled
from dbnd_run.run_executor_engine.local_task_executor import LocalTaskExecutor


if typing.TYPE_CHECKING:

    from dbnd._core.task_run.task_run import TaskRun
    from dbnd_run.run_executor.run_executor import RunExecutor
    from dbnd_run.run_executor_engine import RunExecutorEngine
    from dbnd_run.run_settings import EngineConfig

logger = logging.getLogger(__name__)


def calculate_task_executor_type(submit_tasks, remote_engine, run_config):
    parallel = run_config.parallel
    task_executor_type = run_config.task_executor_type

    dbnd_run_airflow_enabled = is_dbnd_orchestration_via_airflow_enabled()

    if task_executor_type is None:
        if dbnd_run_airflow_enabled:
            from dbnd_run.airflow.executors import AirflowTaskExecutorType

            task_executor_type = AirflowTaskExecutorType.airflow_inprocess
        else:
            task_executor_type = TaskExecutorType.local

    if dbnd_run_airflow_enabled:
        from dbnd_run.airflow.executors import AirflowTaskExecutorType

        if parallel:
            if task_executor_type == TaskExecutorType.local:
                logger.warning(
                    "Auto switching to engine type '%s' due to parallel mode.",
                    AirflowTaskExecutorType.airflow_multiprocess_local,
                )
                task_executor_type = AirflowTaskExecutorType.airflow_multiprocess_local

            if task_executor_type == AirflowTaskExecutorType.airflow_inprocess:
                logger.warning(
                    "Auto switching to engine type '%s' due to parallel mode.",
                    AirflowTaskExecutorType.airflow_multiprocess_local,
                )
                task_executor_type = AirflowTaskExecutorType.airflow_multiprocess_local

        if (
            task_executor_type == AirflowTaskExecutorType.airflow_multiprocess_local
            or task_executor_type == AirflowTaskExecutorType.airflow_kubernetes
        ):
            from dbnd_run.airflow.db_utils import (
                airflow_sql_conn_url,
                airlow_sql_alchemy_conn,
            )

            if "sqlite" in airlow_sql_alchemy_conn():
                if run_config.enable_concurent_sqlite:
                    logger.warning(
                        "You are running parallel execution on top of sqlite database at %s! (see run.enable_concurent_sqlite)",
                        airflow_sql_conn_url(),
                    )
                else:
                    # in theory sqlite can support a decent amount of parallelism, but in practice
                    # the way airflow works each process holds the db exlusively locked which leads
                    # to sqlite DB is locked exceptions
                    raise errors.execute_engine.parallel_or_remote_sqlite(
                        task_executor_type
                    )

        if is_plugin_enabled("dbnd-docker"):
            from dbnd_docker.kubernetes.kubernetes_engine_config import (
                KubernetesEngineConfig,
            )

            if (
                submit_tasks
                and isinstance(remote_engine, KubernetesEngineConfig)
                and run_config.enable_airflow_kubernetes
            ):
                if task_executor_type != AirflowTaskExecutorType.airflow_kubernetes:
                    logger.info("Using dedicated kubernetes executor for this run")
                    task_executor_type = AirflowTaskExecutorType.airflow_kubernetes
                    parallel = True
    else:
        if parallel:
            logger.warning("Airflow is not installed, parallel mode is not supported")

    all_executor_types = [TaskExecutorType.local]
    if dbnd_run_airflow_enabled:
        from dbnd_run.airflow.executors import AirflowTaskExecutorType

        all_executor_types.extend(AirflowTaskExecutorType.all())

    if task_executor_type not in all_executor_types:
        raise DatabandConfigError(
            f"Unsupported engine type '{task_executor_type}' (supported: {all_executor_types})"
        )

    return task_executor_type, parallel


def get_task_executor(
    run_executor: "RunExecutor",
    task_executor_type: str,
    host_engine: "EngineConfig",
    target_engine: "EngineConfig",
    task_runs: "List[TaskRun]",
) -> "RunExecutorEngine":

    if task_executor_type == TaskExecutorType.local:
        return LocalTaskExecutor(
            run_executor,
            task_executor_type=task_executor_type,
            host_engine=host_engine,
            target_engine=target_engine,
            task_runs=task_runs,
        )
    else:

        from dbnd_run.airflow.dbnd_task_executor.dbnd_task_executor_via_airflow import (
            AirflowTaskExecutor,
        )

        return AirflowTaskExecutor(
            run_executor,
            task_executor_type=task_executor_type,
            host_engine=host_engine,
            target_engine=target_engine,
            task_runs=task_runs,
        )
