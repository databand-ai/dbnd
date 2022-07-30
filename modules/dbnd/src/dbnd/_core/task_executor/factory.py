# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from dbnd._core.constants import TaskExecutorType
from dbnd._core.errors import DatabandConfigError, friendly_error
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled, is_plugin_enabled
from dbnd._core.task_executor.local_task_executor import LocalTaskExecutor


if typing.TYPE_CHECKING:
    from typing import List

    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.settings import EngineConfig
    from dbnd._core.task_executor.task_executor import TaskExecutor
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


def calculate_task_executor_type(submit_tasks, remote_engine, settings):
    run_config = settings.run
    parallel = run_config.parallel
    task_executor_type = run_config.task_executor_type

    if task_executor_type is None:
        if is_airflow_enabled():
            from dbnd_airflow.executors import AirflowTaskExecutorType

            task_executor_type = AirflowTaskExecutorType.airflow_inprocess
        else:
            task_executor_type = TaskExecutorType.local

    if is_airflow_enabled():
        from dbnd_airflow.executors import AirflowTaskExecutorType

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
            from dbnd_airflow.db_utils import (
                airflow_sql_conn_url,
                airlow_sql_alchemy_conn,
            )

            if "sqlite" in airlow_sql_alchemy_conn():
                if settings.run.enable_concurent_sqlite:
                    logger.warning(
                        "You are running parallel execution on top of sqlite database at %s! (see run.enable_concurent_sqlite)",
                        airflow_sql_conn_url(),
                    )
                else:
                    # in theory sqlite can support a decent amount of parallelism, but in practice
                    # the way airflow works each process holds the db exlusively locked which leads
                    # to sqlite DB is locked exceptions
                    raise friendly_error.execute_engine.parallel_or_remote_sqlite(
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
    if is_airflow_enabled():
        from dbnd_airflow.executors import AirflowTaskExecutorType

        all_executor_types.extend(AirflowTaskExecutorType.all())

    if task_executor_type not in all_executor_types:
        raise DatabandConfigError("Unsupported engine type %s" % task_executor_type)

    return task_executor_type, parallel


def get_task_executor(run, task_executor_type, host_engine, target_engine, task_runs):
    # type: (DatabandRun, str, EngineConfig, EngineConfig, List[TaskRun]) -> TaskExecutor

    if task_executor_type == TaskExecutorType.local:
        return LocalTaskExecutor(
            run,
            task_executor_type=task_executor_type,
            host_engine=host_engine,
            target_engine=target_engine,
            task_runs=task_runs,
        )
    else:
        from dbnd_airflow.dbnd_task_executor.dbnd_task_executor_via_airflow import (
            AirflowTaskExecutor,
        )

        return AirflowTaskExecutor(
            run,
            task_executor_type=task_executor_type,
            host_engine=host_engine,
            target_engine=target_engine,
            task_runs=task_runs,
        )
