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
import signal
import time
import typing

from airflow.contrib.executors.kubernetes_executor import (
    AirflowKubernetesScheduler,
    KubeConfig,
    KubernetesExecutor,
    KubernetesJobWatcher,
)
from airflow.models import KubeWorkerIdentifier
from airflow.utils.db import provide_session
from airflow.utils.state import State

from dbnd._core.constants import TaskRunState
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors.base import DatabandRuntimeError, DatabandSigTermError
from dbnd._core.utils.basics.signal_utils import safe_signal
from dbnd_airflow_contrib.kubernetes_metrics_logger import KubernetesMetricsLogger
from dbnd_docker.kubernetes.kube_dbnd_client import (
    DbndKubernetesClient,
    get_task_run_from_pod_data,
)


if typing.TYPE_CHECKING:
    from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig

MAX_POD_ID_LEN = 253

logger = logging.getLogger(__name__)


def _update_airflow_kube_config(airflow_kube_config, engine_config):
    # type:( KubeConfig, KubernetesEngineConfig) -> None
    # We (almost) don't need this mapping any more
    # Pod is created using  databand KubeConfig
    # We still are mapping databand KubeConfig -> airflow KubeConfig as some functions are using values from it.
    ec = engine_config

    secrets = ec.get_secrets(include_system_secrets=True)
    if secrets:
        kube_secrets = {}
        env_from_secret_ref = []
        for s in secrets:
            if s.deploy_type == "env":
                if s.deploy_target:
                    kube_secrets[s.deploy_target] = "%s=%s" % (s.secret, s.key)
                else:
                    env_from_secret_ref.append(s.secret)

        if kube_secrets:
            airflow_kube_config.kube_secrets.update(kube_secrets)

        if env_from_secret_ref:
            airflow_kube_config.env_from_secret_ref = ",".join(env_from_secret_ref)

    if ec.env_vars is not None:
        airflow_kube_config.kube_env_vars.update(ec.env_vars)

    if ec.configmaps is not None:
        airflow_kube_config.env_from_configmap_ref = ",".join(ec.configmaps)

    if ec.container_repository is not None:
        airflow_kube_config.worker_container_repository = ec.container_repository
    if ec.container_tag is not None:
        airflow_kube_config.worker_container_tag = ec.container_tag
    airflow_kube_config.kube_image = "{}:{}".format(
        airflow_kube_config.worker_container_repository,
        airflow_kube_config.worker_container_tag,
    )

    if ec.image_pull_policy is not None:
        airflow_kube_config.kube_image_pull_policy = ec.image_pull_policy
    if ec.node_selectors is not None:
        airflow_kube_config.kube_node_selectors.update(ec.node_selectors)
    if ec.annotations is not None:
        airflow_kube_config.kube_annotations.update(ec.annotations)

    if ec.pods_creation_batch_size is not None:
        airflow_kube_config.worker_pods_creation_batch_size = (
            ec.pods_creation_batch_size
        )
    if ec.service_account_name is not None:
        airflow_kube_config.worker_service_account_name = ec.service_account_name
    if ec.image_pull_secrets is not None:
        airflow_kube_config.image_pull_secrets = ec.image_pull_secrets

    if ec.namespace is not None:
        airflow_kube_config.kube_namespace = ec.namespace
    if ec.namespace is not None:
        airflow_kube_config.executor_namespace = ec.namespace

    if ec.gcp_service_account_keys is not None:
        airflow_kube_config.gcp_service_account_keys = ec.gcp_service_account_keys
    if ec.affinity is not None:
        airflow_kube_config.kube_affinity = ec.affinity
    if ec.tolerations is not None:
        airflow_kube_config.kube_tolerations = ec.tolerations


class DbndKubernetesScheduler(AirflowKubernetesScheduler):
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
        # will be used to low level pod interactions
        self.failed_pods_to_ignore = []
        self.running_pods = {}
        self.pod_to_task = {}
        self.metrics_logger = KubernetesMetricsLogger()

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
        self.running_pods[pod.name] = self.namespace
        self.pod_to_task[pod.name] = task_run.task

        pod_ctrl.run_pod(pod=pod, task_run=task_run, detach_run=True)
        self.metrics_logger.log_pod_started(task_run.task)

    def delete_pod(self, pod_id):
        if pod_id in self.failed_pods_to_ignore:
            logger.warning(
                "Received request to delete pod %s that is ignored! Ignoring...", pod_id
            )
            return
        try:
            found_pod = self.running_pods.pop(pod_id, None)
            if found_pod:
                result = self.kube_dbnd.delete_pod(pod_id, self.namespace)

                if pod_id in self.pod_to_task:
                    self.metrics_logger.log_pod_deleted(self.pod_to_task[pod_id])
                    self.pod_to_task.pop(pod_id)  # Keep the cache clean

                return result
        except Exception as e:
            # Catch all exceptions to prevent any delete loops, best effort
            logger.warning(
                "Exception raised when trying to delete pod %s! Adding to ignored list...",
                pod_id,
            )
            self.failed_pods_to_ignore.append(pod_id)

    def terminate(self):
        # we kill watcher and communication channel first
        super(DbndKubernetesScheduler, self).terminate()

        # now we need to clean after the run
        pods_to_delete = sorted(self.running_pods.keys())
        if pods_to_delete:
            logger.info(
                "Terminating run, deleting all %d submitted pods that are still running: %s",
                len(pods_to_delete),
                pods_to_delete,
            )
            pod_to_task_run = {}
            for pod_name in pods_to_delete:
                try:
                    pod_ctrl = self.kube_dbnd.get_pod_ctrl(name=pod_name)
                    pod_data = pod_ctrl.get_pod_status_v1()
                    task_run = get_task_run_from_pod_data(pod_data)
                    if task_run:
                        pod_to_task_run[pod_name] = task_run
                    self.delete_pod(pod_name)
                except Exception:
                    logger.exception("Failed to terminate pod %s", pod_name)
            try:
                self.set_running_pods_to_cancelled(pod_to_task_run)
            except Exception:
                logger.exception("Could not set pods to cancelled!")

    def set_running_pods_to_cancelled(self, pod_to_task_run):
        # Wait for pods to be deleted and execute their own state management
        logger.info("Scheduler: Setting all running pods to cancelled in 10 seconds...")
        time.sleep(10)
        for pod_name, task_run in pod_to_task_run.items():
            self.kube_dbnd.dbnd_set_task_cancelled_on_termination(pod_name, task_run)


def mgr_sig_handler(signal, frame):
    logger.error("Kubernetes python SyncManager got SIGINT (waiting for .stop command)")


def watcher_sig_handler(signal, frame):
    import sys

    logger.info("Watcher received signal, exiting...")
    sys.exit(0)


def mgr_init():
    safe_signal(signal.SIGINT, mgr_sig_handler)


class DbndKubernetesExecutor(KubernetesExecutor):
    def __init__(self, kube_dbnd=None):
        # type: (DbndKubernetesExecutor, DbndKubernetesClient) -> None
        from os import environ

        # This env variable is required for airflow's kubernetes configuration validation
        environ["AIRFLOW__KUBERNETES__DAGS_IN_IMAGE"] = "True"
        super(DbndKubernetesExecutor, self).__init__()

        from multiprocessing.managers import SyncManager

        self._manager = SyncManager()

        self.kube_dbnd = kube_dbnd
        _update_airflow_kube_config(
            airflow_kube_config=self.kube_config, engine_config=kube_dbnd.engine_config
        )

    def start(self):
        logger.info("Starting Kubernetes executor..")
        self._manager.start(mgr_init)

        dbnd_run = try_get_databand_run()
        if dbnd_run:
            self.worker_uuid = str(dbnd_run.run_uid)
        else:
            self.worker_uuid = (
                KubeWorkerIdentifier.get_or_create_current_kube_worker_uuid()
            )
        self.log.debug("Start with worker_uuid: %s", self.worker_uuid)

        # always need to reset resource version since we don't know
        # when we last started, note for behavior below
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs
        # /CoreV1Api.md#list_namespaced_pod
        # KubeResourceVersion.reset_resource_version()
        self.task_queue = self._manager.Queue()
        self.result_queue = self._manager.Queue()

        self.kube_client = self.kube_dbnd.kube_client
        self.kube_scheduler = DbndKubernetesScheduler(
            self.kube_config,
            self.task_queue,
            self.result_queue,
            self.kube_client,
            self.worker_uuid,
            kube_dbnd=self.kube_dbnd,
        )

        if self.kube_dbnd.engine_config.debug:
            self.log.setLevel(logging.DEBUG)
            self.kube_scheduler.log.setLevel(logging.DEBUG)

        self._inject_secrets()
        self.clear_not_launched_queued_tasks()
        self._flush_result_queue()

    # override - by default UpdateQuery not working failing with
    # sqlalchemy.exc.CompileError: Unconsumed column names: state
    # due to model override
    # + we don't want to change tasks statuses - maybe they are managed by other executors
    @provide_session
    def clear_not_launched_queued_tasks(self, *args, **kwargs):
        # we don't clear kubernetes tasks from previous run
        pass


class DbndKubernetesJobWatcher(KubernetesJobWatcher):
    def __init__(self, kube_dbnd, **kwargs):
        super(DbndKubernetesJobWatcher, self).__init__(**kwargs)
        self.kube_dbnd = kube_dbnd  # type: DbndKubernetesClient
        self.processed_events = {}
        self.processed_pods = {}
        self.metrics_logger = KubernetesMetricsLogger()

    def run(self):
        """Performs watching"""
        # we are in the different process than Scheduler
        # 1. Must reset filesystem cache to avoid using out-of-cluster credentials within Kubernetes
        self.reset_fs_cache()
        # 2. Must reset signal handlers to avoid driver and watcher sharing signal handlers

        signal.signal(signal.SIGINT, watcher_sig_handler)
        signal.signal(signal.SIGTERM, watcher_sig_handler)
        signal.signal(signal.SIGQUIT, watcher_sig_handler)

        kube_client = self.kube_dbnd.kube_client
        try:
            while True:
                try:
                    self.resource_version = self._run(
                        kube_client,
                        self.resource_version,
                        self.worker_uuid,
                        self.kube_config,
                    )
                except DatabandSigTermError:
                    break
                except Exception:
                    self.log.exception("Unknown error in KubernetesJobWatcher. Failing")
                    raise
                else:
                    self.log.info(
                        "KubernetesWatcher restarting with resource_version: %s in %s seconds",
                        self.resource_version,
                        self.kube_dbnd.engine_config.watcher_recreation_interval_seconds,
                    )
                    time.sleep(
                        self.kube_dbnd.engine_config.watcher_recreation_interval_seconds
                    )
        except (KeyboardInterrupt, DatabandSigTermError):
            pass

    def _run(self, kube_client, resource_version, worker_uuid, kube_config):
        self.log.info(
            "Event: and now my watch begins starting at resource_version: %s",
            resource_version,
        )

        from kubernetes import watch

        watcher = watch.Watch()
        request_timeout = self.kube_dbnd.engine_config.watcher_request_timeout_seconds
        kwargs = {
            "label_selector": "airflow-worker={}".format(worker_uuid),
            "_request_timeout": (request_timeout, request_timeout),
            "timeout_seconds": self.kube_dbnd.engine_config.watcher_client_timeout_seconds,
        }
        if resource_version:
            kwargs["resource_version"] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        for event in watcher.stream(
            kube_client.list_namespaced_pod, self.namespace, **kwargs
        ):
            try:
                # DBND PATCH
                # we want to process the message
                task = event["object"]
                self.log.debug(
                    " %s had an event of type %s", task.metadata.name, event["type"],
                )

                if event["type"] == "ERROR":
                    return self.process_error(event)

                pod_data = event["object"]
                pod_name = pod_data.metadata.name
                phase = pod_data.status.phase

                if self.processed_events.get(pod_name):
                    self.log.debug(
                        "Event: %s at %s - skipping as seen", phase, pod_name
                    )
                    continue
                status = self.kube_dbnd.process_pod_event(event)

                self._update_node_name(pod_name, pod_data)

                if status in ["Succeeded", "Failed"]:
                    self.processed_events[pod_name] = status

                self.process_status_quite(
                    task.metadata.name,
                    status,
                    task.metadata.labels,
                    task.metadata.resource_version,
                )
                self.resource_version = task.metadata.resource_version

            except Exception as e:
                self.log.warning(
                    "Event: Exception raised on specific event: %s, Exception: %s",
                    event,
                    e,
                )
        return self.resource_version

    def process_error(self, event):
        # Overriding airflow's order of operation to prevent redundant error logs (no actual error, just reset
        # resource version)
        raw_object = event["raw_object"]
        if raw_object["code"] == 410:
            self.log.info(
                "Kubernetes resource version is too old, resetting to 0 => %s",
                (raw_object["message"],),
            )
            # Return resource version 0
            return "0"
        raise DatabandRuntimeError(
            "Kubernetes failure for %s with code %s and message: %s"
            % (raw_object["reason"], raw_object["code"], raw_object["message"])
        )

    def _update_node_name(self, pod_name, pod_data):
        if self.processed_pods.get(pod_name):
            self.log.debug("Pod %s has already been logged to metrics - skipping")
            return
        node_name = pod_data.spec.node_name
        if not node_name:
            return
        # Some events are missing the node name, but it will get there for sure
        try:
            task_id = pod_data.metadata.labels.get("task_id")
            if not task_id:
                return

            dr = try_get_databand_run()
            if not dr:
                return
            task_run = dr.get_task_run(task_id)
            if not task_run:
                return

            self.metrics_logger.log_pod_information(task_run.task, pod_name, node_name)
        except Exception as ex:
            logger.info("Failed to gather node name for %s", pod_name)
        finally:
            self.processed_pods[pod_name] = True

    def process_status_quite(self, pod_id, status, labels, resource_version):
        """Process status response"""
        if status == "Pending":
            self.log.debug("Event: %s Pending", pod_id)
        elif status == "Failed":
            self.log.debug("Event: %s Failed", pod_id)
            self.watcher_queue.put((pod_id, State.FAILED, labels, resource_version))
        elif status == "Succeeded":
            self.log.debug("Event: %s Succeeded", pod_id)
            self.watcher_queue.put((pod_id, State.SUCCESS, labels, resource_version))
        elif status == "Running":
            self.log.debug("Event: %s is Running", pod_id)
        else:
            self.log.warning(
                "Event: Invalid state: %s on pod: %s with labels: %s with "
                "resource_version: %s",
                status,
                pod_id,
                labels,
                resource_version,
            )

    def reset_fs_cache(self):
        from targets.fs import reset_fs_cache

        reset_fs_cache()
