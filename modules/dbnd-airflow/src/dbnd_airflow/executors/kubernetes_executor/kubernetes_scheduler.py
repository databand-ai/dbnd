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


import logging
import time

from airflow.contrib.executors.kubernetes_executor import AirflowKubernetesScheduler
from airflow.utils.state import State

from dbnd._core.constants import TaskRunState
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors.friendly_error.executor_k8s import KubernetesImageNotFoundError
from dbnd_airflow.airflow_extensions.dal import (
    get_airflow_task_instance,
    get_airflow_task_instance_state,
    schedule_task_instance_for_retry,
    update_airflow_task_instance_in_db,
)
from dbnd_airflow.executors.kubernetes_executor.kubernetes_watcher import (
    DbndKubernetesJobWatcher,
)
from dbnd_airflow.executors.kubernetes_executor.utils import mgr_init
from dbnd_airflow_contrib.kubernetes_metrics_logger import KubernetesMetricsLogger
from dbnd_docker.kubernetes.kube_dbnd_client import (
    PodRetryReason,
    _get_status_log_safe,
    _try_get_pod_exit_code,
    get_task_run_from_pod_data,
)


logger = logging.getLogger(__name__)


class DbndKubernetesScheduler(AirflowKubernetesScheduler):
    """
    Very serious override of AirflowKubernetesScheduler
    1. we want better visability on errors, so we proceed Failures with much more info
    2. handling of disappeared pods
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

        # Scheduler <-> (via _manager) KubeWatcher
        # if _manager dies inplace, we will not get any "info" from KubeWatcher until shutdown
        self._manager = SyncManager()
        self._manager.start(mgr_init)

        self.watcher_queue = self._manager.Queue()
        self.current_resource_version = 0
        self.kube_watcher = self._make_kube_watcher_dbnd()

        # pod to airflow key (dag_id, task_id, execution_date)
        self.running_pods = {}
        self.pod_to_task_run = {}

        self.metrics_logger = KubernetesMetricsLogger()

        # disappeared pods mechanism
        self.last_disappeared_pods = {}
        self.current_iteration = 1

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
        watcher = DbndKubernetesJobWatcher(
            namespace=self.namespace,
            watcher_queue=self.watcher_queue,
            resource_version=self.current_resource_version,
            worker_uuid=self.worker_uuid,
            kube_config=self.kube_config,
            kube_dbnd=self.kube_dbnd,
        )
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
                "dag_id": self._make_safe_label_value(dag_id),
                "task_id": self._make_safe_label_value(task_run.task_af_id),
                "execution_date": self._datetime_to_label_safe_datestring(
                    execution_date
                ),
                "try_number": str(try_number),
            },
            try_number=try_number,
            include_system_secrets=True,
        )

        pod_ctrl = self.kube_dbnd.get_pod_ctrl_for_pod(pod)
        self.running_pods[pod.name] = key
        self.pod_to_task_run[pod.name] = task_run

        pod_ctrl.run_pod(pod=pod, task_run=task_run, detach_run=True)
        self.metrics_logger.log_pod_started(task_run.task)

    def delete_pod(self, pod_id):
        # we will try to delete pod only once

        self.running_pods.pop(pod_id, None)
        task_run = self.pod_to_task_run.pop(pod_id, None)
        if not task_run:
            return

        try:
            self.metrics_logger.log_pod_finished(task_run.task)
        except Exception:
            # Catch all exceptions to prevent any delete loops, best effort
            logger.exception("Failed to save pod finish info: pod_name=%s.!", pod_id)

        try:
            result = self.kube_dbnd.delete_pod(pod_id, self.namespace)
            return result
        except Exception:
            # Catch all exceptions to prevent any delete loops, best effort
            logger.exception(
                "Exception raised when trying to delete pod: pod_name=%s.", pod_id
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
        pods_to_delete = sorted(list(self.pod_to_task_run.items()))
        if not pods_to_delete:
            return

        logger.info(
            "Terminating run, deleting all %d submitted pods that are still running.",
            len(pods_to_delete),
        )
        for pod_name, task_run in pods_to_delete:
            try:
                self.delete_pod(pod_name)
            except Exception:
                logger.exception("Failed to terminate pod %s", pod_name)

        # Wait for pods to be deleted and execute their own state management
        logger.info("Scheduler: Setting all running pods to cancelled in 10 seconds...")
        time.sleep(10)
        try:
            for pod_name, task_run in pods_to_delete:
                self._dbnd_set_task_cancelled_on_termination(pod_name, task_run)
        except Exception:
            logger.exception("Could not set pods to cancelled!")

    def _dbnd_set_task_cancelled_on_termination(self, pod_name, task_run):
        if task_run.task_run_state in TaskRunState.final_states():
            logger.info(
                "pod %s was %s, not setting to cancelled",
                pod_name,
                task_run.task_run_state,
            )
            return
        task_run.set_task_run_state(TaskRunState.CANCELLED)

    def process_watcher_task(self, task):
        """Process the task by watcher."""
        pod_id, state, labels, resource_version = task
        self.log.info(
            "Attempting to process pod; pod_name: %s; state: %s; labels: %s",
            pod_id,
            state,
            labels,
        )
        key = self._labels_to_key(labels=labels)
        if key:
            self._handle_pod_events(pod_id, key, state, labels)

            self.log.debug("finishing job %s - %s (%s)", key, state, pod_id)
            self.result_queue.put((key, state, pod_id, resource_version))

    def _handle_pod_events(self, pod_name, key, state, labels):

        task_run = self.pod_to_task_run.get(pod_name)
        if not task_run:
            logger.info("Can't find a task run for %s", pod_name)
            return

        if state is None:
            # simple case, pod has success - will be proceed by airflow main scheduler (Job)
            self._dbnd_set_task_success(pod_name=pod_name, task_run=task_run)
            return state

        pod_data = self.get_pod_status(pod_name)

        if state == State.RUNNING:
            logger.info("Event: %s is Running", pod_name)
            self._dbnd_set_task_running(task_run, pod_name=pod_name, pod_data=pod_data)
        elif state == State.FAILED:
            self._dbnd_set_task_failed(
                pod_name=pod_name, task_run=task_run, pod_data=pod_data
            )

    def _dbnd_set_task_running(self, task_run, pod_name, pod_data):
        node_name = pod_data.spec.node_name
        if not node_name:
            return

        self.metrics_logger.log_pod_running(
            task_run.task, pod_name, node_name=node_name
        )

    def _dbnd_set_task_success(self, task_run, pod_name):
        logger.debug("Getting task run")

        if task_run.task_run_state == TaskRunState.SUCCESS:
            logger.info("Skipping 'success' event from %s", pod_name)
            return

        # let just notify the success, so we can show it in summary it
        # we will not send it to databand tracking store
        task_run.set_task_run_state(TaskRunState.SUCCESS, track=False)
        logger.info(
            "%s",
            task_run.task.ctrl.banner(
                "Task %s has been completed at pod '%s'!"
                % (task_run.task.task_name, pod_name),
                color="green",
                task_run=task_run,
            ),
        )

    def _dbnd_set_task_failed(self, task_run, pod_name, pod_data):
        pod_phase = pod_data.status.phase
        pod_ctrl = self.kube_dbnd.get_pod_ctrl(name=pod_name)

        if pod_phase == "Pending":
            logger.info(
                "Got pod %s at Pending state which is failing: looking for the reason..",
                pod_name,
            )
            try:
                pod_ctrl.check_deploy_errors(pod_data)
            except KubernetesImageNotFoundError as ex:
                self._dbnd_set_task_pending_fail(pod_data, ex)
            except Exception as ex:
                self._dbnd_set_task_pending_fail(pod_data, ex)
            return

        metadata = pod_data.metadata
        if task_run.task_run_state == TaskRunState.FAILED:
            logger.info("Skipping 'failure' event from %s", pod_name)
            return

        pod_ctrl = self.kube_dbnd.get_pod_ctrl(pod_name, metadata.namespace)
        err_msg = "Pod %s at %s has failed!" % (pod_name, metadata.namespace)

        pod_logs = pod_ctrl.get_pod_logs()
        if pod_logs:
            err_msg += "\nLog:\n"
            err_msg += "\n".join(["out:%s" % l for l in pod_logs[:-20]])

        status_log = _get_status_log_safe(pod_data)
        if status_log:
            err_msg += "\nPod Status:%s" % status_log

        from dbnd._core.task_run.task_run_error import TaskRunError

        error = TaskRunError.build_from_message(
            task_run=task_run,
            msg=err_msg,
            help_msg="Please see full pod log for more details",
        )

        airflow_task_state = get_airflow_task_instance_state(task_run=task_run)
        logger.debug("task airflow state: %s ", airflow_task_state)

        if airflow_task_state == State.FAILED:
            # let just notify the error, so we can show it in summary it
            # we will not send it to databand tracking store
            task_run.set_task_run_state(TaskRunState.FAILED, track=False, error=error)
            logger.info(
                "%s",
                task_run.task.ctrl.banner(
                    "Task %s has failed at pod '%s'!"
                    % (task_run.task.task_name, pod_name),
                    color="red",
                    task_run=task_run,
                ),
            )
            return

        if self._handle_pod_failure(
            pod_ctrl, pod_data, task_run, airflow_task_state=airflow_task_state
        ):
            logger.info("Pod %s is restarted!", task_run)
        else:
            # This code is reached when check_for_retry is false, currently from terminate() only
            task_run.set_task_run_state(TaskRunState.FAILED, track=True, error=error)
        if pod_logs:
            task_run.tracker.save_task_run_log("\n".join(pod_logs))

    def _dbnd_set_task_pending_fail(self, pod_data, ex):
        metadata = pod_data.metadata

        task_run = get_task_run_from_pod_data(pod_data)
        if not task_run:
            return
        from dbnd._core.task_run.task_run_error import TaskRunError

        task_run_error = TaskRunError.build_from_ex(ex, task_run)

        status_log = _get_status_log_safe(pod_data)
        logger.info(
            "Pod '%s' is Pending with exception, marking it as failed. Pod Status:\n%s",
            metadata.name,
            status_log,
        )
        task_run.set_task_run_state(TaskRunState.FAILED, error=task_run_error)
        task_instance = get_airflow_task_instance(task_run)
        from airflow.utils.state import State

        task_instance.state = State.FAILED
        update_airflow_task_instance_in_db(task_instance)
        task_run.tracker.save_task_run_log(status_log)

    def _handle_pod_failure(
        self, pod_ctrl, pod_data, task_run, airflow_task_state,
    ):
        metadata = pod_data.metadata
        pod_name = metadata.name

        increment_try_number = False
        if airflow_task_state == State.QUEUED:
            # Special case - no airflow code has been run in the pod at all. Must increment try number and send
            # to retry if exit code is matching
            increment_try_number = True
        elif airflow_task_state == State.RUNNING:
            # Task was killed unexpectedly -- probably pod failure in K8s - Possible retry attempt
            pass
        else:
            return False

        pod_exit_code = _try_get_pod_exit_code(pod_data)
        if not pod_exit_code:
            # Couldn't find an exit code - container is still alive - wait for the next event
            logger.debug("No exit code found for pod %s, doing nothing", pod_name)
            return False
        logger.info("Found pod exit code %d for pod %s", pod_exit_code, pod_name)
        return self.try_to_retry_task(
            retry_reason=str(pod_exit_code),
            pod_name=pod_name,
            task_run=task_run,
            increment_try_number=increment_try_number,
        )

    def try_to_retry_task(
        self, retry_reason, pod_name, task_run, increment_try_number,
    ):
        retry_config = self.kube_config.pod_retry_config
        if str(retry_reason) not in retry_config.reasons_and_exit_codes:
            return False
        retry_count = retry_config.get_retry_count(retry_reason)
        retry_delay = retry_config.get_retry_delay(retry_reason)

        if not retry_count:
            return False
        logger.info(
            "Reached handle pod retry for pod %s for reason %s!", pod_name, retry_reason
        )

        if schedule_task_instance_for_retry(
            task_run, retry_count, retry_delay, increment_try_number
        ):
            task_run.set_task_run_state(TaskRunState.UP_FOR_RETRY, track=True)
            logger.info(
                "Scheduling the pod %s for %s retries with delay of %s",
                pod_name,
                retry_count,
                retry_delay,
            )
            return True
        else:
            logger.warning(
                "Pod %s was not scheduled for retry because it reached the maximum retry limit"
            )
            return False

    def sync(self):
        super(DbndKubernetesScheduler, self).sync()

        # DBND-AIRFLOW: doing more validations during the sync
        # we do it after kube_scheduler sync, so all "finished pods" are removed already
        # but no new pod requests are submitted
        self.current_iteration += 1

        if self.current_iteration % 10 == 0:
            try:
                self.handle_disappeared_pods()
            except Exception:
                logger.exception("Failed to find disappeared pods")

    #######
    # HANDLE DISAPPEARED PODS
    #######

    def handle_disappeared_pods(self):
        pods = self.__find_disappeared_pods()
        if not pods:
            return
        logger.info(
            "Pods %s can not be found for the last 2 iterations of disappeared pods recovery. "
            "Trying to recover..",
            pods,
        )
        for pod_name in pods:
            task_run = self.pod_to_task_run.get(pod_name)
            key = self.running_pods.get(pod_name)

            if self.try_to_retry_task(
                pod_name=pod_name,
                task_run=task_run,
                retry_reason=PodRetryReason.err_pod_disappeared,
                increment_try_number=True,
            ):
                self.result_queue.put((key, State.UP_FOR_RETRY, pod_name, None))
            else:
                # pod wasn't resubmitted
                error = None
                task_run.set_task_run_state(
                    TaskRunState.FAILED, track=True, error=error
                )

                self.result_queue.put((key, State.FAILED, pod_name, None))

            self.delete_pod(pod_name)

    def __find_disappeared_pods(self):
        """
        We will want to check on pod status.
        K8s may have pods disappeared from it because of preemptable/spot nodes
         without proper event sent to KubernetesJobWatcher
        We will do a check on all running pods every 10th iteration
        :return:
        """
        if not self.running_pods:
            self.last_disappeared_pods = {}
            logger.info(
                "Skipping on checking on disappeared pods - no pods are running"
            )
            return

        logger.info(
            "Checking on disappeared pods for currently %s running tasks",
            len(self.running_pods),
        )

        previously_disappeared_pods = self.last_disappeared_pods
        currently_disappeared_pods = {}
        running_pods = list(
            self.running_pods.items()
        )  # need a copy, will be modified on delete
        disapeared_pods = []
        for pod_name, pod_key in running_pods:
            from kubernetes.client.rest import ApiException

            try:
                self.kube_client.read_namespaced_pod(
                    name=pod_name, namespace=self.namespace
                )
            except ApiException as e:
                # If the pod can not be found
                if e.status == 404:
                    logger.info("Pod %s has disappeared...", pod_name)
                    if pod_name in previously_disappeared_pods:
                        disapeared_pods.append(pod_name)
                    currently_disappeared_pods[pod_name] = pod_key
            except Exception:
                logger.exception("Failed to get status of pod_name=%s", pod_name)
        self.last_disappeared_pods = currently_disappeared_pods

        if disapeared_pods:
            logger.info("Disappeared pods: %s ", disapeared_pods)
        return disapeared_pods

    def get_pod_status(self, pod_name):
        pod_ctrl = self.kube_dbnd.get_pod_ctrl(name=pod_name)
        return pod_ctrl.get_pod_status_v1()
