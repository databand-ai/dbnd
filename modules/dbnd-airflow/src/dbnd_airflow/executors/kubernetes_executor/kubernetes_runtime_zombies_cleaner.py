# © Copyright Databand.ai, an IBM Company 2022

import datetime
import typing

from typing import List

import six

from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import or_
from sqlalchemy.orm import Session

from dbnd._core.log.logging_utils import PrefixLoggerAdapter
from dbnd_airflow.airflow_extensions.dal import get_airflow_task_instance
from dbnd_airflow.compat.airflow_multi_version_shim import LoggingMixin
from dbnd_airflow.constants import AIRFLOW_VERSION_2


if typing.TYPE_CHECKING:
    from dbnd_airflow.executors.kubernetes_executor.kubernetes_executor import (
        DbndKubernetesExecutor,
    )


class ClearKubernetesRuntimeZombiesForDagRun(LoggingMixin):
    """
    workaround for SingleDagRunJob to clean "missing" tasks
    for now it works for kubernetes only,
    as k8s executor requires "explicit" _clean_state for "zombie" tasks
    otherwise, they will not be submitted (as they are still at executior.running tasks
    """

    def __init__(self, k8s_executor):
        super(ClearKubernetesRuntimeZombiesForDagRun, self).__init__()

        self._last_zombie_query_time = None
        self.k8s_executor = k8s_executor  # type: DbndKubernetesExecutor

        # time configurations
        self.zombie_threshold_secs = (
            k8s_executor.kube_dbnd.engine_config.zombie_threshold_secs
        )
        self.zombie_query_interval_secs = (
            k8s_executor.kube_dbnd.engine_config.zombie_query_interval_secs
        )
        self._pending_zombies_timeout = (
            k8s_executor.kube_dbnd.engine_config.pending_zombies_timeout
        )

        self._log = PrefixLoggerAdapter("clear-zombies", self.log)

    @provide_session
    def find_and_clean_dag_zombies(self, dag, execution_date, session):
        # type: (DAG, datetime.datetime, Session) -> None
        now = timezone.utcnow()
        if (
            self._last_zombie_query_time
            and (now - self._last_zombie_query_time).total_seconds()
            < self.zombie_query_interval_secs
        ):
            return

        self._last_zombie_query_time = timezone.utcnow()
        self.log.debug("Checking on possible zombie tasks")
        zombies = self._find_task_instance_zombies(dag, execution_date, session=session)
        if not zombies:
            return

        self.log.info("Found %s zombie tasks.", len(zombies))
        self._kill_zombies(dag, zombies=zombies, session=session)

    def _kill_zombies(self, dag, zombies, session):
        """
        copy paste from airflow.models.dagbag.DagBag.kill_zombies
        """
        for zombie in zombies:
            if zombie.task_id not in dag.task_ids:
                continue  # old implementation, can't happen in SingleJobRun

            self.k8s_executor.clear_zombie_task_instance(zombie_task_instance=zombie)
            # original zombie implementation,
            # we just call zombie handling at k8s scheduler
            #
            # task = dag.get_task(zombie.task_id)
            # ti = TaskInstance(task, zombie.execution_date)
            # # Get properties needed for failure handling from SimpleTaskInstance.
            # ti.start_date = zombie.start_date
            # ti.end_date = zombie.end_date
            # ti.try_number = zombie.try_number
            # ti.state = zombie.state
            # ti.test_mode = self.UNIT_TEST_MODE

        session.commit()

    @provide_session
    def _find_task_instance_zombies(self, dag, execution_date, session):
        # type: (DAG, datetime.datetime, Session) -> List[TaskInstance]

        """
        Find zombie task instances, which are tasks haven't send heartbeat for too long
        and update the current zombie list.

        copy paste from DagFileProcessorAgent
        """
        zombies = []

        # finding running task without actual heartbeat - they seems alive but they are not
        running_zombies = self._find_running_zombies(dag, execution_date, session)
        zombies.extend(running_zombies)

        # finding pending pods without actual pod
        pending_zombies = self._find_pending_zombies(session)
        zombies.extend(pending_zombies)

        return zombies

    def _find_running_zombies(self, dag, execution_date, session):
        # type: (DAG, datetime.datetime, Session) -> List[TaskInstance]
        """
        Find tasks tha airflow think they running but actually didn't sent any
        heartbeat for too long
        """
        # to avoid circular imports
        if AIRFLOW_VERSION_2:
            from airflow.jobs.local_task_job import LocalTaskJob as LJ
        else:
            from airflow.jobs import LocalTaskJob as LJ

        limit_dttm = timezone.utcnow() - datetime.timedelta(
            seconds=self.zombie_threshold_secs
        )

        running_zombies = (
            session.query(TaskInstance)
            .join(LJ, TaskInstance.job_id == LJ.id)
            .filter(TaskInstance.state == State.RUNNING)
            .filter(TaskInstance.dag_id == dag.dag_id)
            .filter(TaskInstance.execution_date == execution_date)
            .filter(or_(LJ.state != State.RUNNING, LJ.latest_heartbeat < limit_dttm))
            .all()
        )

        if running_zombies:
            self.log.warning("Failing jobs without heartbeat after %s", limit_dttm)
            self.log.warning(
                "Detected running zombies task instances: \n\t\t\t%s",
                "\n\t\t\t".join(self._build_ti_msg(ti) for ti in running_zombies),
            )

        return running_zombies

    def _find_pending_zombies(self, session):
        # type: (Session) -> List[TaskInstance]
        """
        Find pods that are on `pending` state but disappeared

        this is very unique scenario where:
            1) Pod was pending
            2) The pod disappear and we didn't see any event telling us that the pod failed

        More info:
            https://app.asana.com/0/1141064349624642/1200130408884044/f
        """
        now = timezone.utcnow()
        pending_zombies = []

        for pod_name, pod_state in six.iteritems(
            self.k8s_executor.kube_scheduler.submitted_pods
        ):
            # we look for a state where the pod is pending for too long
            if (
                not pod_state.is_started_running
                and (now - pod_state.submitted_at) >= self._pending_zombies_timeout
            ):
                pod_status = self.k8s_executor.kube_dbnd.get_pod_status(pod_name)
                if pod_status is None:
                    # the pod doesn't exit anymore so its a zombie pending
                    af_ti = get_airflow_task_instance(
                        pod_state.task_run, session=session
                    )
                    pending_zombies.append(af_ti)

        if pending_zombies:
            self.log.warning(
                "Failing pending pods for more than {timeout}".format(
                    timeout=self._pending_zombies_timeout
                )
            )
            self.log.warning(
                "Detected pending zombies pods for task instance: \n\t\t\t%s",
                "\n\t\t\t".join(self._build_ti_msg(ti) for ti in pending_zombies),
            )

        return pending_zombies

    @staticmethod
    def _build_ti_msg(ti):
        return "dag_id=%s task_id=%s execution_date= %s, try_number=%s" % (
            ti.dag_id,
            ti.task_id,
            ti.execution_date.isoformat(),
            ti.try_number,
        )
