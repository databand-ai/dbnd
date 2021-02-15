# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import time
import typing

from typing import Dict

import attr

from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.timezone import utcnow

from dbnd._core.constants import TaskRunState
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors.friendly_error.executor_k8s import KubernetesImageNotFoundError
from dbnd._core.log.logging_utils import PrefixLoggerAdapter
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd_airflow.airflow_extensions.dal import (
    get_airflow_task_instance,
    get_airflow_task_instance_state,
)
from dbnd_airflow.compat.kubernetes_executor import (
    AirflowKubernetesScheduler,
    get_job_watcher_kwargs,
    make_safe_label_value,
)
from dbnd_airflow.executors.kubernetes_executor.kubernetes_watcher import (
    DbndKubernetesJobWatcher,
)
from dbnd_airflow.executors.kubernetes_executor.utils import mgr_init
from dbnd_airflow_contrib.kubernetes_metrics_logger import KubernetesMetricsLogger
from dbnd_docker.kubernetes.kube_dbnd_client import (
    PodFailureReason,
    _get_status_log_safe,
    _try_get_pod_exit_code,
)


if typing.TYPE_CHECKING:
    from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


@attr.s
class SubmittedPodState(object):
    pod_name = attr.ib()
    submitted_at = attr.ib()

    # key from airflow (task instance
    scheduler_key = attr.ib()
    # dbnd run
    task_run = attr.ib()

    # state
    # returned to KubernetesExecutor -> Main Scheduler
    # but probably is still not deleted
    processed = attr.ib(default=False)

    # set on running
    node_name = attr.ib(default=None)

    @property
    def try_number(self):
        return self.scheduler_key[1]


class DbndKubernetesScheduler(AirflowKubernetesScheduler):
    """
    Very serious override of AirflowKubernetesScheduler
    1. better visability on errors, so we proceed Failures with much more info
    2. tracking of all around "airflow run" events -> Pod Crashes, Pod Submission errors
        a. in case of crash (OOM, evicted pod) -> error propogation to databand and retry
    """

    def __init__(
        self, kube_config, task_queue, result_queue, kube_client, worker_uuid, kube_dbnd
    ):
        super(DbndKubernetesScheduler, self).__init__(
            kube_config, task_queue, result_queue, kube_client, worker_uuid
        )
        self.kube_dbnd = kube_dbnd

        # PATCH watcher communication manager
        # we want to wait for stop, instead of "exit" inplace, so we can get all "not" received messages
        from multiprocessing.managers import SyncManager

        # TODO: why can't we use original SyncManager?
        # Scheduler <-> (via _manager) KubeWatcher
        # if _manager dies inplace, we will not get any "info" from KubeWatcher until shutdown
        self._manager = SyncManager()
        self._manager.start(mgr_init)

        self.watcher_queue = self._manager.Queue()
        self.current_resource_version = 0
        self.kube_watcher = self._make_kube_watcher_dbnd()

        # pod to airflow key (dag_id, task_id, execution_date)
        self.submitted_pods = {}  # type: Dict[str,SubmittedPodState]

        # sending data to databand tracker
        self.metrics_logger = KubernetesMetricsLogger()

        # disappeared pods mechanism
        self.last_disappeared_pods = {}
        self.current_iteration = 1
        # add `k8s-scheduler:` prefix to all log messages
        self._log = PrefixLoggerAdapter("k8s-scheduler", self.log)

    def _make_kube_watcher(self):
        # prevent storing in db of the kubernetes resource version, because the kubernetes db model only stores a single value
        # of the resource version while we need to store a sperate value for every kubernetes executor (because even in a basic flow
        # we can have two Kubernets executors running at once, the one that launched the driver and the one inside the driver).
        #
        # the resource version is the position inside the event stream of the kubernetes cluster and is used by the watcher to poll
        # Kubernets for events. It's probably fine to not store this because by default Kubernetes will returns "the evens currently in cache"
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md#list_namespaced_pod
        return None

    def _make_kube_watcher_dbnd(self):
        watcher = DbndKubernetesJobWatcher(**get_job_watcher_kwargs(self))
        watcher.start()
        return watcher

    @staticmethod
    def _create_pod_id(dag_id, task_id):
        task_run = try_get_databand_run().get_task_run(task_id)
        return task_run.job_id__dns1123

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            pass
        else:
            self.log.error(
                "Error while health checking kube watcher process. "
                "Process died for unknown reasons"
            )
            self.kube_watcher = self._make_kube_watcher_dbnd()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevant info in the current_jobs map so we can track the job's
        status
        """
        key, command, kube_executor_config = next_job
        dag_id, task_id, execution_date, try_number = key
        self.log.debug(
            "Kube POD to submit: image=%s with %s",
            self.kube_config.kube_image,
            str(next_job),
        )

        dr = try_get_databand_run()
        task_run = dr.get_task_run_by_af_id(task_id)
        pod_command = [str(c) for c in command]
        task_engine = task_run.task_engine  # type: KubernetesEngineConfig
        pod = task_engine.build_pod(
            task_run=task_run,
            cmds=pod_command,
            labels={
                "airflow-worker": self.worker_uuid,
                "dag_id": make_safe_label_value(dag_id),
                "task_id": make_safe_label_value(task_run.task_af_id),
                "execution_date": self._datetime_to_label_safe_datestring(
                    execution_date
                ),
                "try_number": str(try_number),
            },
            try_number=try_number,
            include_system_secrets=True,
        )

        pod_ctrl = self.kube_dbnd.get_pod_ctrl_for_pod(pod)
        self.submitted_pods[pod.name] = SubmittedPodState(
            pod_name=pod.name,
            task_run=task_run,
            scheduler_key=key,
            submitted_at=utcnow(),
        )

        pod_ctrl.run_pod(pod=pod, task_run=task_run, detach_run=True)
        self.metrics_logger.log_pod_submitted(task_run.task, pod_name=pod.name)

    # in airflow>1.10.10 delete_pod method takes additional "namespace" arg
    # we do not use it in our overridden method but still we need to adjust
    # method signature to avoid errors when we run code on airflow>1.10.10.
    def delete_pod(self, pod_id, *args):
        # we are going to delete pod only once.
        # the moment it's removed from submitted_pods, we will not handle it event, neither delete it
        submitted_pod = self.submitted_pods.pop(pod_id, None)
        if not submitted_pod:
            return

        try:
            self.metrics_logger.log_pod_finished(submitted_pod.task_run.task)
        except Exception:
            # Catch all exceptions to prevent any delete loops, best effort
            self.log.exception(
                "%s failed to save pod finish info: pod_name=%s.!",
                submitted_pod.task_run,
                pod_id,
            )

        try:
            result = self.kube_dbnd.delete_pod(pod_id, self.namespace)
            return result
        except Exception:
            # Catch all exceptions to prevent any delete loops, best effort
            self.log.exception(
                "%s: Exception raised when trying to delete pod: pod_name=%s.",
                submitted_pod.task_run,
                pod_id,
            )

    def terminate(self):
        # we kill watcher and communication channel first

        # prevent watcher bug of being stacked on termination during event processing
        try:
            self.kube_watcher.safe_terminate()
            super(DbndKubernetesScheduler, self).terminate()
        finally:
            self._terminate_all_running_pods()

    def _terminate_all_running_pods(self):
        """
        Clean up of all running pods on terminate:
        """
        # now we need to clean after the run
        pods_to_delete = sorted(list(self.submitted_pods.values()))
        if not pods_to_delete:
            return

        self.log.info(
            "Terminating run, deleting all %d submitted pods that are still running/not finalized",
            len(pods_to_delete),
        )
        for submitted_pod in pods_to_delete:
            try:
                self.delete_pod(submitted_pod.pod_name)
            except Exception:
                self.log.exception("Failed to terminate pod %s", submitted_pod.pod_name)

        # Wait for pods to be deleted and execute their own state management
        self.log.info(
            "Setting all running/not finalized pods to cancelled in 10 seconds..."
        )
        time.sleep(10)
        try:
            for submitted_pod in pods_to_delete:
                task_run = submitted_pod.task_run
                if task_run.task_run_state in TaskRunState.final_states():
                    self.log.info(
                        "%s with pod %s was %s, skipping",
                        task_run,
                        submitted_pod.pod_name,
                        task_run.task_run_state,
                    )
                    continue
                task_run.set_task_run_state(TaskRunState.CANCELLED)
        except Exception:
            self.log.exception("Could not set pods to cancelled!")

    def process_watcher_task(self, task):
        """Process the task event sent by watcher."""
        pod_id, state, labels, resource_version = task
        pod_name = pod_id
        self.log.debug(
            "Attempting to process pod; pod_name: %s; state: %s; labels: %s",
            pod_id,
            state,
            labels,
        )

        submitted_pod = self.submitted_pods.get(pod_name)
        if submitted_pod is None:
            # this is deleted pod - on delete watcher will send event
            # 1. delete by scheduler - we skip here
            # 2. external delete -> we continue to process the event
            return

        # DBND-AIRFLOW we have it precached, we don't need to go to DB
        # key = self._labels_to_key(labels=labels)
        # if not key:
        #     self.log.info(
        #         "Can't find a key for event from %s - %s from labels %s, skipping",
        #         pod_name,
        #         state,
        #         labels,
        #     )
        #     return

        self.log.debug(
            "Attempting to process pod; pod_name: %s; state: %s; labels: %s",
            pod_id,
            state,
            labels,
        )

        # we are not looking for key
        task_run = submitted_pod.task_run
        key = submitted_pod.scheduler_key
        if submitted_pod.processed:
            # we already processed this kind of event, as in this process we have failed status already
            self.log.info(
                "%s Skipping pod '%s' event from %s - already processed",
                state,
                pod_name,
            )
            return

        if state == State.RUNNING:
            # we should get here only once -> when pod starts to run

            self._process_pod_running(submitted_pod)
            # we will not send event to executor (otherwise it will delete the running pod)
            return

        try:
            if state is None:
                # simple case, pod has success - will be proceed by airflow main scheduler (Job)
                # task can be failed or passed. Airflow exit with 0 if task has failed regular way.
                self._process_pod_success(submitted_pod)
                self.result_queue.put((key, state, pod_name, resource_version))
            elif state == State.FAILED:
                # Pod crash, it was deleted, killed, evicted.. we need to give it extra treatment
                self._process_pod_failed(submitted_pod)
                self.result_queue.put((key, state, pod_id, resource_version))
            else:
                self.log.debug("finishing job %s - %s (%s)", key, state, pod_id)
                self.result_queue.put((key, state, pod_id, resource_version))
        finally:
            submitted_pod.processed = True

    def _process_pod_running(self, submitted_pod):
        task_run = submitted_pod.task_run
        pod_name = submitted_pod.pod_name

        if submitted_pod.node_name:
            self.log.info(
                "%s: Zombie bug: Seeing pod event again. "
                "Probably something happening with pod and it's node: %s",
                submitted_pod.task_run,
                submitted_pod.pod_name,
            )
            return

        pod_data = self.get_pod_status(pod_name)
        if not pod_data or not pod_data.spec.node_name:
            self.log.error("%s: Failed to find pod data for %s", pod_name)
            node_name = "failed_to_find"
        else:
            node_name = pod_data.spec.node_name
            self.metrics_logger.log_pod_running(task_run.task, node_name=node_name)

        submitted_pod.node_name = node_name
        task_run.set_task_run_state(TaskRunState.RUNNING, track=False)

    def _process_pod_success(self, submitted_pod):
        task_run = submitted_pod.task_run
        pod_name = submitted_pod.pod_name

        if submitted_pod.processed:
            self.log.info(
                "%s Skipping pod 'success' event from %s: already processed", pod_name
            )
            return
        ti = get_airflow_task_instance(task_run=task_run)

        # we print success message to the screen
        # we will not send it to databand tracking store

        if ti.state == State.SUCCESS:
            dbnd_state = TaskRunState.SUCCESS
        elif ti.state in {State.UP_FOR_RETRY, State.UP_FOR_RESCHEDULE}:
            dbnd_state = TaskRunState.UP_FOR_RETRY
        elif ti.state in {State.FAILED, State.SHUTDOWN}:
            dbnd_state = TaskRunState.FAILED
        else:
            # we got a corruption here:
            error_msg = (
                "Pod %s has finished with SUCCESS, but task instance state is %s, failing the job."
                % (pod_name, ti.state)
            )
            error_help = "Please check pod logs/eviction retry"
            task_run_error = TaskRunError.build_from_message(
                task_run, error_msg, help_msg=error_help
            )
            self._handle_crashed_task_instance(
                failure_reason=PodFailureReason.err_pod_evicted,
                task_run_error=task_run_error,
                task_run=task_run,
            )
            return

        task_run.set_task_run_state(dbnd_state, track=False)
        self.log.info(
            "%s has been completed at pod '%s' with state %s try_number=%s!"
            % (task_run, pod_name, ti.state, ti._try_number)
        )

    def _process_pod_failed(self, submitted_pod):
        task_run = submitted_pod.task_run
        pod_name = submitted_pod.pod_name

        task_id = task_run.task_af_id
        ti_state = get_airflow_task_instance_state(task_run=task_run)

        self.log.info(
            "%s: pod %s has crashed, airflow state: %s", task_run, pod_name, ti_state
        )

        pod_data = self.get_pod_status(pod_name)
        pod_ctrl = self.kube_dbnd.get_pod_ctrl(pod_name, self.namespace)

        pod_logs = []
        if pod_data:
            pod_status_log = _get_status_log_safe(pod_data)
            pod_phase = pod_data.status.phase
            if pod_phase != "Pending":
                pod_logs = pod_ctrl.get_pod_logs()
        else:
            pod_status_log = "POD NOT FOUND"

        error_msg = "Pod %s at %s has failed (task state=%s)!" % (
            pod_name,
            self.namespace,
            ti_state,
        )
        failure_reason, failure_message = self._find_pod_failure_reason(
            task_run=task_run, pod_data=pod_data, pod_name=pod_name
        )
        if failure_reason:
            error_msg += "Found reason for failure: %s - %s." % (
                failure_reason,
                failure_message,
            )
        error_help_msg = "Please see full pod log for more details."
        if pod_logs:
            error_help_msg += "\nPod logs:\n%s\n" % "\n".join(
                ["out: %s" % l for l in pod_logs[-20:]]
            )

        from dbnd._core.task_run.task_run_error import TaskRunError

        task_run_error = TaskRunError.build_from_message(
            task_run=task_run, msg=error_msg, help_msg=error_help_msg,
        )

        if ti_state == State.FAILED:
            # Pod has failed, however, Airfow managed to update the state
            # that means - all code (including dbnd) were executed
            # let just notify the error, so we can show it in summary it
            # we will not send it to databand tracking store
            task_run.set_task_run_state(
                TaskRunState.FAILED, track=False, error=task_run_error
            )
            self.log.info(
                "%s",
                task_run.task.ctrl.banner(
                    "Task %s(%s) - pod %s has failed, airlfow state=Failed!"
                    % (task_run.task.task_name, task_id, pod_name),
                    color="red",
                    task_run=task_run,
                ),
            )
            return True
        # we got State.Failed from watcher, but at DB airflow instance in different state
        # that means the task has failed in the middle
        # (all kind of errors and exit codes)
        task_run_log = error_msg
        task_run_log += pod_status_log
        if pod_logs:
            # let's upload it logs - we don't know what happen
            task_run_log += "\nPod logs:\n\n%s\n\n" % "\n".join(pod_logs)
        task_run.tracker.save_task_run_log(task_run_log)

        self._handle_crashed_task_instance(
            task_run=task_run,
            task_run_error=task_run_error,
            failure_reason=failure_reason,
        )

    def _find_pod_failure_reason(
        self, task_run, pod_name, pod_data,
    ):
        if not pod_data:
            return (
                PodFailureReason.err_pod_deleted,
                "Pod %s probably has been deleted (can not be found)" % pod_name,
            )

        pod_phase = pod_data.status.phase
        pod_ctrl = self.kube_dbnd.get_pod_ctrl(name=pod_name)

        if pod_phase == "Pending":
            self.log.info(
                "Got pod %s at Pending state which is failing: looking for the reason..",
                pod_name,
            )
            try:
                pod_ctrl.check_deploy_errors(pod_data)
            except KubernetesImageNotFoundError as ex:
                return PodFailureReason.err_image_pull, str(ex)
            except Exception as ex:
                pass
            return None, None

        if pod_data.metadata.deletion_timestamp:
            return (
                PodFailureReason.err_pod_deleted,
                "Pod %s has been deleted at %s"
                % (pod_name, pod_data.metadata.deletion_timestamp),
            )

        pod_exit_code = _try_get_pod_exit_code(pod_data)
        if pod_exit_code:
            self.log.info("Found pod exit code %d for pod %s", pod_exit_code, pod_name)
            pod_exit_code = str(pod_exit_code)
            return pod_exit_code, "Pod exit code %s" % pod_exit_code
        return None, None

    @provide_session
    def _handle_crashed_task_instance(
        self, task_run, task_run_error, failure_reason, session=None
    ):

        task_instance = get_airflow_task_instance(task_run, session=session)
        task_instance.task = task_run.task.ctrl.airflow_op

        retry_config = self.kube_dbnd.engine_config.pod_retry_config
        retry_count = retry_config.get_retry_count(failure_reason)
        if retry_count is not None:
            # update retry for the latest values (we don't have
            task_run.task.task_retries = retry_count
            task_instance.task.retries = retry_count
            task_instance.max_tries = retry_count

        self.log.info(
            "Retry %s  task: max_retries=%s, task.retries=%s, current:%s state:%s",
            task_run,
            task_instance.max_tries,
            task_instance.task.retries,
            task_instance._try_number,
            task_instance.state,
        )
        # retry condition: self.task.retries and self.try_number <= self.max_tries
        increase_try_number = False

        if task_instance.state == State.QUEUED:
            # Special case - no airflow code has been run in the pod at all.
            # usually its increased the momen state moved to Running. And while at running state -> it will be the same value
            # Must increment try number,
            task_instance._try_number += 1
            session.merge(task_instance)
            session.commit()

        task_instance.handle_failure(str(task_run_error.exception), session=session)

        if task_instance.state == State.UP_FOR_RETRY:
            task_run.set_task_run_state(
                TaskRunState.UP_FOR_RETRY, track=True, error=task_run_error
            )
        else:
            task_run.set_task_run_state(
                TaskRunState.FAILED, track=True, error=task_run_error
            )

    def get_pod_status(self, pod_name):
        pod_ctrl = self.kube_dbnd.get_pod_ctrl(name=pod_name)
        return pod_ctrl.get_pod_status_v1()
