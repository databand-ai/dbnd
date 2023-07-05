# © Copyright Databand.ai, an IBM Company 2022

import logging
import pprint
import time
import typing

from datetime import datetime
from typing import Optional

from kubernetes import client
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

from dbnd._core.constants import TaskRunState
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors import DatabandError, DatabandRuntimeError
from dbnd._core.log.logging_utils import PrefixLoggerAdapter, override_log_formatting
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils.timezone import utcnow
from dbnd_docker.kubernetes.compat.pod_launcher import PodStatus
from dbnd_docker.kubernetes.kube_resources_checker import DbndKubeResourcesChecker
from dbnd_docker.kubernetes.kubernetes_engine_config import (
    KubernetesEngineConfig,
    readable_pod_request,
)
from dbnd_run import errors
from dbnd_run.errors import executor_k8s


if typing.TYPE_CHECKING:
    import kubernetes.client.models as k8s

    from kubernetes.client import CoreV1Api, V1Pod

    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


class PodFailureReason(object):
    err_image_pull = "err_image_pull"
    err_config_error = "err_config_error"
    err_pod_deleted = "err_pod_deleted"
    err_pod_evicted = "err_pod_evicted"


class DbndKubernetesClient(object):
    def __init__(self, kube_client, engine_config):
        # type:(DbndKubernetesClient, CoreV1Api,KubernetesEngineConfig)->None
        super(DbndKubernetesClient, self).__init__()

        self.kube_client = kube_client
        self.engine_config = engine_config

    def get_pod_ctrl(self, name, namespace=None, config=None):
        # type: (str, Optional[str], Optional[KubernetesEngineConfig])-> DbndPodCtrl
        return DbndPodCtrl(
            pod_name=name,
            pod_namespace=namespace or self.engine_config.namespace,
            kube_client=self.kube_client,
            kube_config=config or self.engine_config,
        )

    def delete_pod(self, name, namespace):
        self.get_pod_ctrl(name=name, namespace=namespace).delete_pod()

    def get_pod_status(self, pod_name):
        # type: (str) -> Optional[V1Pod]
        return self.get_pod_ctrl(name=pod_name).get_pod_status_v1()


class DbndPodCtrl(object):
    def __init__(self, pod_name, pod_namespace, kube_config, kube_client):
        self.kube_config = kube_config  # type: KubernetesEngineConfig
        self.name = pod_name
        self.namespace = pod_namespace
        self.kube_client = kube_client
        self.log = PrefixLoggerAdapter("pod %s" % self.name, logger)

    def delete_pod(self):
        if self.kube_config.keep_finished_pods:
            self.log.warning("Will not delete pod due to keep_finished_pods=True.")
            return

        if self.kube_config.keep_failed_pods:
            pod_phase = self.get_pod_phase()
            if pod_phase not in {PodPhase.RUNNING, PodPhase.SUCCEEDED}:
                self.log.warning(
                    "Keeping failed pod due to keep_failed_pods=True and state is %s",
                    pod_phase,
                )
                return

        try:
            self.kube_client.delete_namespaced_pod(
                self.name, self.namespace, body=client.V1DeleteOptions()
            )
            self.log.info("Pod has been deleted.")
        except ApiException as e:
            self.log.info(
                "Failed to delete pod: %s", e if e.status != 404 else "pod not found"
            )
            # If the pod is already deleted, don't raise
            # if e.status != 404:
            #     raise

    def get_pod_status_v1(self):
        # type: () -> Optional[V1Pod]
        try:
            return self.kube_client.read_namespaced_pod(
                name=self.name, namespace=self.namespace
            )
        except ApiException as e:
            # If the pod can not be found
            if e.status == 404:
                return None
            raise

    def get_pod_phase(self):
        pod_resp = self.get_pod_status_v1()
        if not pod_resp:
            return None

        return pod_resp.status.phase

    def _wait_for_pod_started(self, _logger=None):
        """
        will try to raise an exception if the pod fails to start (see DbndPodLauncher.check_deploy_errors)
        """
        _logger = _logger or self.log
        start_time = datetime.now()
        while True:
            pod_status = self.get_pod_status_v1()
            if not pod_status:
                raise DatabandError("Can not find pod at k8s:%s")
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

    def stream_pod_logs_with_retries(self) -> bool:
        seconds_to_sleep_before_retries = 5
        try_counter = 0
        max_retries = self.kube_config.max_retries_on_log_stream_failure
        while try_counter < 100:
            try:
                last_x_seconds = None
                if try_counter > 0:
                    last_x_seconds = seconds_to_sleep_before_retries * 2
                logger.debug(
                    f"Starting to stream logs; follow=True; since_seconds: %s",
                    last_x_seconds,
                )
                self.stream_pod_logs(follow=True, since_seconds=last_x_seconds)
                return True
            except HTTPError as ex:
                # Log streaming relies on blocking network request.
                # We want to restart and block until the end of pod execution in case of network disconnection.
                if try_counter >= max_retries:
                    self.log.exception(
                        "Failed to stream logs: %s; Max retries exceeded or kubernetes.max_retries_on_log_stream_failure setting is 0.",
                        self.name,
                    )
                    return False

                self.log.warning(
                    "Failed to stream logs: %s, continuing after 5 seconds; try %d out of %d",
                    self.name,
                    try_counter,
                    max_retries,
                    exc_info=ex,
                )
                time.sleep(seconds_to_sleep_before_retries)
                try_counter += 1
                continue
            except Exception:
                self.log.exception("Failed to stream logs:  %s", self.name)
                return False

    def stream_pod_logs(
        self,
        print_func=logger.info,
        follow: bool = False,
        tail_lines=None,
        since_seconds: Optional[int] = None,
    ):
        kwargs = {
            "name": self.name,
            "namespace": self.namespace,
            "container": "base",
            "follow": follow,
            "_preload_content": False,
        }
        if tail_lines:
            kwargs["tail_lines"] = tail_lines

        if since_seconds:
            kwargs["since_seconds"] = since_seconds

        logs = self.kube_client.read_namespaced_pod_log(**kwargs)
        if self.kube_config.prefix_remote_log:
            # we want to remove regular header in log, and make it looks like '[pod_name] LOG FROM POD'
            prefix = "[%s]" % self.name
            with override_log_formatting(prefix + "%(message)s"):
                for line in logs:
                    print_func(line[:-1].decode("utf-8"))
        else:
            for line in logs:
                print_func(line[:-1].decode("utf-8"))

    def check_deploy_errors(self, pod_v1_resp):
        pod_status = pod_v1_resp.status
        if self.kube_config.check_unschedulable_condition and pod_status.conditions:
            for condition in pod_status.conditions:
                if condition.reason != "Unschedulable":
                    continue
                logger.info("pod is pending because %s" % condition.message)
                if (
                    "Insufficient cpu" in condition.message
                    or "Insufficient memory" in condition.message
                ):
                    if self.kube_config.check_cluster_resource_capacity:
                        kube_resources_checker = DbndKubeResourcesChecker(
                            kube_client=self.kube_client, kube_config=self.kube_config
                        )
                        kube_resources_checker.check_if_resource_request_above_max_capacity(
                            condition.message
                        )

                    self.log.warning("pod is pending because %s" % condition.message)
                else:
                    raise executor_k8s.kubernetes_pod_unschedulable(condition.message)

        if pod_status.container_statuses:
            container_waiting_state = pod_status.container_statuses[0].state.waiting
            if (
                self.kube_config.check_image_pull_errors
                and pod_status.phase == "Pending"
                and container_waiting_state
            ):
                if container_waiting_state.reason == "ErrImagePull":
                    raise executor_k8s.kubernetes_image_not_found(
                        pod_status.container_statuses[0].image,
                        container_waiting_state.message,
                        long_msg=container_waiting_state.reason,
                    )

                if container_waiting_state.reason == "CreateContainerConfigError":
                    raise executor_k8s.kubernetes_pod_config_error(
                        container_waiting_state.message
                    )

    def check_running_errors(self, pod_v1_resp):
        """
        Raise an error if pod in running state with Failed conditions
        """
        pod_status = pod_v1_resp.status
        if not self.kube_config.check_running_pod_errors:
            return
        if pod_status.conditions:
            for condition in pod_status.conditions:
                if condition.type != "Ready":
                    continue
                # We are looking for
                #  {
                #   u"status": u"False",
                #   u"lastProbeTime": None,
                #   u"type": u"Ready",
                #   u"lastTransitionTime": u"2021-01-22T04:54:13Z",
                #  },
                if not condition.status or condition.status == "False":
                    raise errors.executor_k8s.kubernetes_running_pod_fails_on_condition(
                        condition, pod_name=pod_v1_resp.metadata.name
                    )
                return True
        return False

    def wait(self):
        """
        Waits for pod completion
        :return:
        """
        self._wait_for_pod_started()
        self.log.info("Pod is running, reading logs..")
        self.stream_pod_logs_with_retries()
        self.log.info("Successfully read pod logs")

        pod_phase = self.get_pod_phase()
        wait_start = utcnow()
        while pod_phase not in {PodPhase.SUCCEEDED, PodPhase.FAILED}:
            logger.debug(
                "Pod '%s' is not completed with state %s, waiting..",
                self.name,
                pod_phase,
            )
            if (
                utcnow() - wait_start
            ) > self.kube_config.submit_termination_grace_period:
                raise DatabandRuntimeError(
                    "Pod is not in a final state after {grace_period}: {state}".format(
                        grace_period=self.kube_config.submit_termination_grace_period,
                        state=pod_phase,
                    )
                )
            time.sleep(5)
            pod_phase = self.get_pod_phase()

        if pod_phase != PodPhase.SUCCEEDED:
            raise DatabandRuntimeError(
                "Pod returned a failure: {pod_phase}".format(pod_phase=pod_phase)
            )
        return self

    def run_pod(
        self, task_run: "TaskRun", pod: "k8s.V1Pod", detach_run: bool = False
    ) -> "DbndPodCtrl":
        kc = self.kube_config
        detach_run = detach_run or kc.detach_run
        if not self.is_possible_to_detach_run():
            detach_run = False

        req = kc.build_kube_pod_req(pod)
        self._attach_live_logs_container(req)

        readable_req_str = readable_pod_request(req)

        if kc.debug:
            logger.info("Pod Creation Request: \n%s", readable_req_str)
            pod_file = task_run.task_run_attempt_file("pod.yaml")
            pod_file.write(readable_req_str)
            logger.debug("Pod Request has been saved to %s", pod_file)

        external_link_dict = self.build_external_links(pod)
        if external_link_dict:
            task_run.set_external_resource_urls(external_link_dict)

        task_run.set_task_run_state(TaskRunState.QUEUED)

        try:
            resp = self.kube_client.create_namespaced_pod(
                body=req, namespace=pod.metadata.namespace
            )
            logger.info(
                "%s has been submitted at pod '%s' at namespace '%s'"
                % (task_run, pod.metadata.name, pod.metadata.namespace)
            )
            self.log.debug("Pod Creation Response: %s", resp)
        except ApiException as ex:
            task_run_error = TaskRunError.build_from_ex(ex, task_run)
            task_run.set_task_run_state(TaskRunState.FAILED, error=task_run_error)
            logger.error(
                "Exception when attempting to create Namespaced Pod using: %s",
                readable_req_str,
            )
            raise

        if detach_run:
            return self

        self.wait()
        return self

    def _attach_live_logs_container(self, req: typing.Dict[str, typing.Any]):
        from dbnd_docker.kubernetes.vendorized_airflow.request_factory import (
            DbndPodRequestFactory,
        )

        DbndPodRequestFactory(self.kube_config).attach_logs_container(req)

    def build_external_links(self, pod: "k8s.V1Pod"):
        kc = self.kube_config
        dashboard_url = kc.get_dashboard_link(pod.metadata.namespace, pod.metadata.name)
        pod_log = kc.get_pod_log_link(pod.metadata.namespace, pod.metadata.name)
        external_link_dict = dict()
        if dashboard_url:
            external_link_dict["k8s_dashboard"] = dashboard_url
        if pod_log:
            external_link_dict["pod_log"] = pod_log
        return external_link_dict

    def is_possible_to_detach_run(self):
        kc = self.kube_config
        can_detach_run = True
        if kc.show_pod_log:
            logger.info(
                "%s is True, %s will send every docker in blocking mode",
                "show_pod_logs",
                kc.task_name,
            )
            can_detach_run = False
        if kc.debug:
            logger.info(
                "%s is True, %s will send every docker in blocking mode",
                "debug",
                kc.task_name,
            )
            can_detach_run = False
        return can_detach_run

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
                self.log.info("failed to get log for pod: pod not found")
            else:
                self.log.exception("failed to get log: %s", ex)
        except Exception as ex:
            self.log.error("failed to get log for %s: %s", ex)


def _get_status_log_safe(pod_data):
    try:
        pp = pprint.PrettyPrinter(indent=4)
        logs = pp.pformat(pod_data.status)
        return "Pod Status: %s" % logs
    except Exception as ex:
        return "Pod Status: failed to get %s: %s" % (pod_data.metadata.name, ex)


def _try_get_pod_exit_code(pod_data) -> (str, str):
    found_exit_code = False
    pod_exit_code = None
    termination_reason = None
    if pod_data.status.container_statuses:
        for container_status in pod_data.status.container_statuses:
            # Searching for the container that was terminated
            if container_status.state.terminated:
                pod_exit_code = container_status.state.terminated.exit_code
                termination_reason = container_status.state.terminated.reason
                found_exit_code = True
                if container_status.name == "base":
                    break

        if found_exit_code:
            return pod_exit_code, termination_reason
    return None, None


def get_task_run_from_pod_data(pod_data):
    labels = pod_data.metadata.labels
    if "task_id" not in labels:
        return None
    task_id = labels["task_id"]

    dr = try_get_databand_run()
    if not dr:
        return None

    return dr.get_task_run_by_af_id(task_id)


class PodPhase(object):
    """Status of the PODs"""

    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"
