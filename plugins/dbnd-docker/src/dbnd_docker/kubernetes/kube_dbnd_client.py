import logging
import pprint
import time
import typing

from datetime import datetime
from typing import Optional

from airflow.contrib.kubernetes.pod_launcher import PodStatus
from kubernetes import client
from kubernetes.client.rest import ApiException

from dbnd._core.constants import TaskRunState
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors import DatabandError, DatabandRuntimeError, friendly_error
from dbnd._core.log.logging_utils import override_log_formatting
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils.timezone import utcnow
from dbnd_docker.kubernetes.kube_resources_checker import DbndKubeResourcesChecker
from dbnd_docker.kubernetes.kubernetes_engine_config import (
    KubernetesEngineConfig,
    readable_pod_request,
)


if typing.TYPE_CHECKING:
    from airflow.contrib.kubernetes.pod import Pod
    from dbnd._core.task_run.task_run import TaskRun
    from kubernetes.client import CoreV1Api
logger = logging.getLogger(__name__)


class PodRetryReason(object):
    exit_code = "exit_code"
    err_image_pull = "err_image_pull"
    err_pod_disappeared = "err_pod_disappeared"


class DbndKubernetesClient(object):
    def __init__(self, kube_client, engine_config):
        # type:(DbndKubernetesClient, CoreV1Api,KubernetesEngineConfig)->None
        super(DbndKubernetesClient, self).__init__()

        self.kube_client = kube_client
        self.engine_config = engine_config

    def get_pod_ctrl(self, name, namespace=None):
        # type: (str, Optional[str])-> DbndPodCtrl
        return DbndPodCtrl(
            pod_name=name,
            pod_namespace=namespace or self.engine_config.namespace,
            kube_client=self.kube_client,
            kube_config=self.engine_config,
        )

    def get_pod_ctrl_for_pod(self, pod):
        # type: (Pod)-> DbndPodCtrl
        return self.get_pod_ctrl(pod.name, pod.namespace)

    def delete_pod(self, name, namespace):
        self.get_pod_ctrl(name=name, namespace=namespace).delete_pod()


class DbndPodCtrl(object):
    def __init__(self, pod_name, pod_namespace, kube_config, kube_client):
        self.kube_config = kube_config
        self.name = pod_name
        self.namespace = pod_namespace
        self.kube_client = kube_client

    def delete_pod(self):
        if self.kube_config.keep_finished_pods:
            logger.warning(
                "Will not delete pod '%s' due to keep_finished_pods=True.", self.name
            )
            return

        from airflow.utils.state import State

        if (
            self.kube_config.keep_failed_pods
            and self.get_airflow_state() != State.RUNNING
        ):
            logger.warning(
                "Keeping failed pod '%s' due to keep_failed_pods=True and state is %s",
                self.name,
                self.get_airflow_state(),
            )
            return

        logger.info("Deleting pod: %s" % self.name)

        try:
            self.kube_client.delete_namespaced_pod(
                self.name, self.namespace, body=client.V1DeleteOptions()
            )
            logger.info("Pod '%s' has been deleted", self.name)
        except ApiException as e:
            logger.info(
                "Failed to delete pod '%s': %s",
                self.name,
                e if e.status != 404 else "pod not found",
            )
            # If the pod is already deleted, don't raise
            # if e.status != 404:
            #     raise

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

    def _wait_for_pod_started(self, _logger=logger):
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

    def stream_pod_logs(self, print_func=logger.info, follow=False, tail_lines=None):
        kwargs = {
            "name": self.name,
            "namespace": self.namespace,
            "container": "base",
            "follow": follow,
            "_preload_content": False,
        }
        if tail_lines:
            kwargs["tail_lines"] = tail_lines

        logs = self.kube_client.read_namespaced_pod_log(**kwargs)
        try:
            if self.kube_config.prefix_remote_log:
                # we want to remove regular header in log, and make it looks like '[pod_name] LOG FROM POD'
                prefix = "[%s]" % self.name
                with override_log_formatting(prefix + "%(message)s"):
                    for line in logs:
                        print_func(line[:-1].decode("utf-8"))
            else:
                for line in logs:
                    print_func(line[:-1].decode("utf-8"))
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
                        pod_status.container_statuses[0].image,
                        container_waiting_state.message,
                        long_msg=container_waiting_state.reason,
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

    def wait(self):
        """
        Waits for pod completion
        :return:
        """
        self._wait_for_pod_started()
        logger.info("Pod '%s' is running, reading logs..", self.name)
        self.stream_pod_logs(follow=True)
        logger.info("Successfully read %s pod logs", self.name)

        from airflow.utils.state import State

        final_state = self.get_airflow_state()
        wait_start = utcnow()
        while final_state not in {State.SUCCESS, State.FAILED}:
            logger.debug(
                "Pod '%s' is not completed with state %s, waiting..",
                self.name,
                final_state,
            )
            if (
                utcnow() - wait_start
            ) > self.kube_config.submit_termination_grace_period:
                raise DatabandRuntimeError(
                    "Pod is not in a final state after {grace_period}: {state}".format(
                        grace_period=self.kube_config.submit_termination_grace_period,
                        state=final_state,
                    )
                )
            time.sleep(5)
            final_state = self.get_airflow_state()

        if final_state != State.SUCCESS:
            raise DatabandRuntimeError(
                "Pod returned a failure: {state}".format(state=final_state)
            )
        return self

    def run_pod(self, task_run, pod, detach_run=False):
        # type: (TaskRun, Pod, bool) -> DbndPodCtrl
        kc = self.kube_config

        detach_run = detach_run or kc.detach_run
        if kc.show_pod_log:
            logger.info(
                "%s is True, %s will send every docker in blocking mode",
                "show_pod_logs",
                kc.task_name,
            )
            detach_run = False
        if kc.debug:
            logger.info(
                "%s is True, %s will send every docker in blocking mode",
                "debug",
                kc.task_name,
            )
            detach_run = False

        req = kc.build_kube_pod_req(pod)
        readable_req_str = readable_pod_request(req)

        if kc.debug:
            logger.info("Pod Creation Request: \n%s", readable_req_str)
            pod_file = task_run.task_run_attempt_file("pod.yaml")
            pod_file.write(readable_req_str)
            logger.debug("Pod Request has been saved to %s", pod_file)

        dashboard_url = kc.get_dashboard_link(pod)
        pod_log = kc.get_pod_log_link(pod)
        external_link_dict = dict()
        if dashboard_url:
            external_link_dict["k8s_dashboard"] = dashboard_url
        if pod_log:
            external_link_dict["pod_log"] = pod_log
        if external_link_dict:
            task_run.set_external_resource_urls(external_link_dict)
        task_run.set_task_run_state(TaskRunState.QUEUED)

        try:
            resp = self.kube_client.create_namespaced_pod(
                body=req, namespace=pod.namespace
            )
            logger.info(
                "Started pod '%s' in namespace '%s'" % (pod.name, pod.namespace)
            )
            logger.debug("Pod Creation Response: %s", resp)
        except ApiException as ex:
            task_run_error = TaskRunError.build_from_ex(ex, task_run)
            task_run.set_task_run_state(TaskRunState.FAILED, error=task_run_error)
            logger.error(
                "Exception when attempting to create Namespaced Pod using: %s",
                readable_req_str,
            )
            raise
        logging.debug("Kubernetes Job created!")

        # TODO this is pretty dirty.
        #  Better to extract the deploy error checking logic out of the pod launcher and have the watcher
        #   pass an exception through the watcher queue if needed. Current airflow implementation doesn't implement that, so we will stick with the current flow

        if detach_run:
            return self

        self.wait()
        return self

    def get_pod_logs(self, tail_lines=100):
        try:
            logs = []
            log_printer = lambda x: logs.append(x)
            self.stream_pod_logs(
                print_func=log_printer, tail_lines=tail_lines, follow=False
            )
            return logs
        except ApiException as ex:
            if ex.status == 404:
                logger.info("failed to get log for pod %s: pod not found", self.name)
            else:
                logger.exception("failed to get log for %s: %s", self.name, ex)
        except Exception as ex:
            logger.error("failed to get log for %s: %s", self.name, ex)


def _get_status_log_safe(pod_data):
    if not pod_data:
        return "POD NOT FOUND"
    try:
        pp = pprint.PrettyPrinter(indent=4)
        logs = pp.pformat(pod_data.status)
        return "Pod Status: %s" % logs
    except Exception as ex:
        return "Pod Status: failed to get %s: %s" % (pod_data.metadata.name, ex)


def _try_get_pod_exit_code(pod_data):
    # TODO: look at base container only
    found_exit_code = False
    pod_exit_code = None
    if pod_data.status.container_statuses:
        for container_status in pod_data.status.container_statuses:
            # Searching for the container that was terminated
            if container_status.state.terminated:
                pod_exit_code = container_status.state.terminated.exit_code
                found_exit_code = True
        if found_exit_code:
            return pod_exit_code


def get_task_run_from_pod_data(pod_data):
    labels = pod_data.metadata.labels
    if "task_id" not in labels:
        return None
    task_id = labels["task_id"]

    dr = try_get_databand_run()
    if not dr:
        return None

    return dr.get_task_run_by_af_id(task_id)
