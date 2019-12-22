import logging
import time
import typing

from datetime import datetime

import six

from airflow.contrib.kubernetes.pod_launcher import PodStatus

from dbnd._core.constants import TaskRunState
from dbnd._core.errors import DatabandError, DatabandRuntimeError, friendly_error
from dbnd._core.log.logging_utils import override_log_formatting
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd_docker.kubernetes.kube_resources_checker import DbndKubeResourcesChecker
from dbnd_docker.kubernetes.kubernetes_engine_config import (
    KubernetesEngineConfig,
    readable_pod_request,
)
from kubernetes import client
from kubernetes.client.rest import ApiException


if typing.TYPE_CHECKING:
    from airflow.contrib.kubernetes.pod import Pod
    from dbnd._core.task_run.task_run import TaskRun
    from kubernetes.client import CoreV1Api
logger = logging.getLogger(__name__)


class DbndKubernetesClient(object):
    def __init__(self, kube_client, engine_config):
        # type:(DbndKubernetesClient, CoreV1Api,KubernetesEngineConfig)->None
        super(DbndKubernetesClient, self).__init__()

        self.kube_client = kube_client
        self.engine_config = engine_config

        # will be used to low level pod interactions
        self.running_pods = {}

    def end(self):
        if self.engine_config.delete_pods:
            logger.info("Deleting submitted pods: %s" % self.running_pods)
            for pod_id, pod_namespace in six.iteritems(self.running_pods):
                self.delete_pod(pod_id, pod_namespace)

    def run_pod(self, task_run, pod, run_async=True):
        # type: (TaskRun, Pod, bool) -> DbndPodCtrl
        # we add to running first, so we can prevent racing condition
        self.running_pods[pod.name] = pod.namespace

        ec = self.engine_config
        run_async = run_async and not ec.debug and not ec.show_pod_log

        req = self.engine_config.build_kube_pod_req(pod)
        readable_req_str = readable_pod_request(req)

        if ec.debug:
            logger.info("Pod Creation Request: \n%s", readable_req_str)
            pod_file = task_run.task_run_attempt_file("pod.yaml")
            pod_file.write(readable_req_str)
            logger.debug("Pod Request has been saved to %s", pod_file)

        dashboard_url = ec.get_dashboard_link(pod)
        pod_log = ec.get_pod_log_link(pod)
        external_link_dict = dict()
        if dashboard_url:
            external_link_dict["k8s_dashboard"] = dashboard_url
        if pod_log:
            external_link_dict["pod_log"] = pod_log
        if external_link_dict:
            task_run.set_external_resource_urls(external_link_dict)
        task_run.set_task_run_state(TaskRunState.QUEUED)
        pod_ctrl = self.get_pod_ctrl(pod.name, pod.namespace)
        try:
            resp = self.kube_client.create_namespaced_pod(
                body=req, namespace=pod.namespace
            )
            logger.debug("Pod Creation Response: %s", resp)
        except ApiException as ex:
            task_run_error = TaskRunError.buid_from_ex(ex, task_run)
            task_run.set_task_run_state(TaskRunState.FAILED, error=task_run_error)
            logger.exception(
                "Exception when attempting to create Namespaced Pod using: %s",
                readable_req_str,
            )
            raise
        logging.debug("Kubernetes Job created!")

        # TODO this is pretty dirty.
        #  Better to extract the deploy error checking logic out of the pod launcher and have the watcher
        #   pass an exception through the watcher queue if needed. Current airflow implementation doesn't implement that, so we will stick with the current flow
        pod_ctrl.wait_for_pod_started()

        if not run_async:
            pod_ctrl.stream_pod_logs()

        return pod_ctrl

    def get_pod_ctrl(self, name, namespace):
        return DbndPodCtrl(
            pod_name=name,
            pod_namespace=namespace,
            kube_client=self.kube_client,
            kube_config=self.engine_config,
        )


class DbndPodCtrl(object):
    def __init__(self, pod_name, pod_namespace, kube_config, kube_client):
        self.kube_config = kube_config
        self.name = pod_name
        self.namespace = pod_namespace
        self.kube_client = kube_client

    def delete_pod(self):
        if not self.kube_config.delete_pods:
            logger.info("Will not delete pod '%s' due to delete_pods=False.", self.name)
            return

        from airflow.utils.state import State

        if (
            self.kube_config.keep_failed_pod
            and self.get_airflow_state() == State.FAILED
        ):
            logger.info(
                "Keeping failed pod '%s' due to keep_failed_pods=True.", self.name
            )
            return

        logger.info("Deleting pod: %s" % self.name)

        try:
            self.kube_client.delete_namespaced_pod(
                self.name, self.namespace, body=client.V1DeleteOptions()
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

        if self.kube_config.show_pod_log:
            # noinspection PyBroadException
            try:
                self.stream_pod_logs(log_logger=logger, from_now=True)
            except Exception:
                pass

    def get_pod_status_v1(self):
        from requests import HTTPError

        try:
            return self.kube_client.read_namespaced_pod(self.name, self.namespace)
        except HTTPError as e:
            raise DatabandRuntimeError(
                "There was an error reading pod status for %s at namespace %s via kubernetes API: {}".format(
                    e
                )
            )

    def get_airflow_state(self):
        """Process phase infomration for the JOB"""
        try:
            pod_resp = self.get_pod_status_v1()
            return self._phase_to_airflow_state(pod_resp.status.phase)
        except Exception as e:
            logger.warning("failed to read pod state for %s: %s", self.name, e)
            return None

    def wait_for_pod_started(self, _logger=logger):
        """
        will try to raise an exception if the pod fails to start (see DbndPodLauncher.check_deploy_errors)
        """
        start_time = datetime.now()
        while True:
            pod_status = self.get_pod_status_v1()
            # PATCH:  validate deploy errors
            self.check_deploy_errors(pod_status)

            pod_phase = pod_status.status.phase
            if pod_phase.lower() != PodStatus.PENDING:
                return

            startup_delta = datetime.now() - start_time
            if startup_delta >= self.kube_config.startup_timeout:
                raise DatabandError("Pod is still not running after %s" % startup_delta)
            time.sleep(1)
            _logger.debug("Pod not yet started: %s", pod_status.status)

    def stream_pod_logs(self, log_logger=logger, from_now=False):
        kwargs = {
            "name": self.name,
            "namespace": self.namespace,
            "container": "base",
            "follow": True,
            "tail_lines": 10,
            "_preload_content": False,
        }
        if from_now:
            kwargs["since_seconds"] = 1
        logger.info("driver logs (from Kubernetes):")

        logs = self.kube_client.read_namespaced_pod_log(**kwargs)
        try:
            if self.kube_config.prefix_remote_log:
                # we want to remove regular header in log, and make it looks like '[pod_name] LOG FROM POD'
                prefix = "[%s]" % self.name
                with override_log_formatting(prefix + "%(message)s"):
                    for line in logs:
                        logger.info(line[:-1].decode("utf-8"))
            else:
                for line in logs:
                    logger.info(line[:-1].decode("utf-8"))
            logger.info("end of driver logs")
        except Exception as ex:
            logger.error("Failed to stream logs for %s:  %s", self.name, ex)

    def check_deploy_errors(self, pod_v1_resp):
        pod_status = pod_v1_resp.status
        if pod_status.conditions:
            for condition in pod_status.conditions:
                if (
                    condition.reason == "Unschedulable"
                    and self.kube_config.check_unschedulable_condition
                ):
                    logger.info("pod is pending because %s" % condition.message)
                    if (
                        "Insufficient cpu" in condition.message
                        or "Insufficient memory" in condition.message
                    ):
                        if self.kube_config.check_cluster_resource_capacity:
                            kube_resources_checker = DbndKubeResourcesChecker(
                                kube_client=self.kube_client,
                                kube_config=self.kube_config,
                            )
                            kube_resources_checker.check_if_resource_request_above_max_capacity(
                                condition.message
                            )

                        logger.warning("pod is pending because %s" % condition.message)
                    else:
                        raise friendly_error.executor_k8s.kubernetes_pod_unschedulable(
                            condition.message
                        )

        if pod_status.container_statuses:
            container_waiting_state = pod_status.container_statuses[0].state.waiting
            if pod_status.phase == "Pending" and container_waiting_state:
                if container_waiting_state.reason == "ErrImagePull":
                    raise friendly_error.executor_k8s.kubernetes_image_not_found(
                        pod_status.container_statuses[0].image
                    )

                if container_waiting_state.reason == "CreateContainerConfigError":
                    raise friendly_error.executor_k8s.kubernetes_pod_config_error(
                        container_waiting_state.message
                    )

    def _phase_to_airflow_state(self, pod_phase):
        """Process phase infomration for the JOB"""
        phase = pod_phase.lower()
        from airflow.utils.state import State

        if phase == PodStatus.PENDING:
            return State.QUEUED
        elif phase == PodStatus.FAILED:
            logger.info("Event with pod %s Failed", self.name)
            return State.FAILED
        elif phase == PodStatus.SUCCEEDED:
            logger.info("Event with pod %s Succeeded", self.name)
            return State.SUCCESS
        elif phase == PodStatus.RUNNING:
            return State.RUNNING
        else:
            logger.info("Event: Invalid state %s on job %s", phase, self.name)
            return State.FAILED
