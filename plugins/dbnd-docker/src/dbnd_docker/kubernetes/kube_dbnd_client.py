import logging
import time
import typing

import six
import yaml

from dbnd._core.constants import TaskRunState
from dbnd._core.errors import DatabandConfigError, DatabandRuntimeError, friendly_error
from dbnd._core.log.logging_utils import override_log_formatting
from dbnd._core.utils.json_utils import dumps_safe
from dbnd_airflow.airflow_extensions.request_factory import DbndPodRequestFactory
from dbnd_docker.kubernetes.kubernetes_engine_config import parse_kub_memory_string
from kubernetes.client.rest import ApiException


if typing.TYPE_CHECKING:
    from airflow.contrib.kubernetes.pod import Pod
    from dbnd._core.task_run.task_run import TaskRun
    from kubernetes.client import CoreV1Api
logger = logging.getLogger(__name__)

LIST_NODES_TIMEOUT_S = 3


class DbndKubernetesClient(object):
    def __init__(self, kube_client, engine_config):
        # type:(CoreV1Api,KubernetesEngineConfig)->None
        super(DbndKubernetesClient, self).__init__()

        from dbnd_airflow.executors.kubernetes_executor import DbndPodLauncher

        self.kube_client = kube_client
        self.engine_config = engine_config
        self.launcher = DbndPodLauncher(kube_dbnd=self, kube_client=self.kube_client)
        self.kube_req_factory = (
            self.launcher.kube_req_factory
        )  # type: DbndPodRequestFactory

        self.running_pods = {}

    def run_pod(self, pod, run_async=True, task_run=None):
        # type: (Pod, bool, TaskRun) -> None
        # we add to running first, so we can prevent racing condition
        self.running_pods[pod.name] = pod.namespace

        ec = self.engine_config
        # the watcher will monitor pods, so we do not block.
        kube_req_factory = DbndPodRequestFactory()
        if hasattr(pod, "pod_yaml"):
            kube_req_factory._yaml = pod.pod_yaml

        req = kube_req_factory.create(pod)
        try:
            readable_req_str = yaml.safe_dump(
                req, encoding="utf-8", allow_unicode=True
            ).decode("ascii", errors="ignore")
        except Exception as ex:
            logger.exception(
                "Failed to create readable pod request representation: %s", ex
            )
            readable_req_str = dumps_safe(req)

        if ec.debug and task_run:
            logger.info("Pod Creation Request: \n%s", readable_req_str)
            pod_file = task_run.task_run_attempt_file("pod.yaml")
            pod_file.write(readable_req_str)

            logger.debug("Pod Request has been saved to %s", pod_file)
        if task_run:
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
        try:
            resp = self.kube_client.create_namespaced_pod(
                body=req, namespace=pod.namespace
            )
            logger.debug("Pod Creation Response: %s", resp)
        except ApiException:
            logger.exception(
                "Exception when attempting to create Namespaced Pod using: %s",
                readable_req_str,
            )
            raise

        logging.debug("Kubernetes Job created!")

        # TODO this is pretty dirty. Better to extract the deploy error checking logic out of the pod launcher and have the watcher
        # pass an exception through the watcher queue if needed
        self.wait_for_pod_started(resp, pod)

        if not run_async or ec.debug or ec.show_pod_log:
            self.stream_pod_logs(pod=pod)

    def delete_pod(self, pod):
        logger.info("Deleting pod: %s" % pod.name)
        self.launcher.delete_pod(pod)

        if self.engine_config.show_pod_log:
            # noinspection PyBroadException
            try:
                self.stream_pod_logs(pod, _logger=logger, from_now=True)
            except Exception:
                pass

    def check_if_resource_request_above_max_capacity(self, condition_message):
        if self.resource_request_above_max_capacity_checked:
            return

        try:
            cpu_request = self.get_cpu_request()
            memory_request = self.get_memory_request()

            if cpu_request or memory_request:
                nodes = self.get_cluster_nodes()
                if not nodes:
                    return

                message_components = []
                if cpu_request:
                    nodes_with_cpu = [
                        (node, float(node.status.capacity["cpu"])) for node in nodes
                    ]
                    max_cpu_node = max(nodes_with_cpu, key=lambda nwc: nwc[1])
                    if cpu_request > max_cpu_node[1]:
                        message_components.append(
                            self.get_resource_above_max_capacity_message(
                                param="request_cpu",
                                param_value=self.engine_config.request_cpu,
                                name="CPUs",
                                max_node=max_cpu_node[0].metadata.name,
                                max_capacity=max_cpu_node[0].status.capacity["cpu"],
                            )
                        )

                if memory_request:
                    nodes_with_memory = [
                        (node, parse_kub_memory_string(node.status.capacity["memory"]))
                        for node in nodes
                    ]
                    max_memory_node = max(nodes_with_memory, key=lambda nwm: nwm[1])
                    if memory_request > max_memory_node[1]:
                        message_components.append(
                            self.get_resource_above_max_capacity_message(
                                param="request_memory",
                                param_value=self.engine_config.request_memory,
                                name="memory",
                                max_node=max_memory_node[0].metadata.name,
                                max_capacity=max_memory_node[0].status.capacity[
                                    "memory"
                                ],
                            )
                        )

                if message_components:
                    raise friendly_error.executor_k8s.kubernetes_pod_unschedulable(
                        condition_message, extra_help="\n".join(message_components)
                    )

        except DatabandConfigError as ex:
            raise ex
        except Exception as ex:
            logger.warning(
                "failed to check if the requested pod resources are above maximum cluster capacity: %s"
                % ex.args
            )
        finally:
            # better to fail open and let the user terminate the run if needed than to accidentally block a legitimate case we didn't handle
            self.resource_request_above_max_capacity_checked = True

    def get_resource_above_max_capacity_message(self, **kwargs):
        return (
            "Configured {param} ({param_value}) is larger than the node with most {name} in the cluster ({max_node} with {max_capacity}). "
            "Either lower the {param} or add a node with enought {name} to the cluster."
        ).format(**kwargs)

    def get_cpu_request(self):
        """
        https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-cpu
        :return: float value of requested cpu fractions or None
        """
        try:
            if not self.engine_config.request_cpu:
                return None

            if self.engine_config.request_cpu[-1].lower() == "m":
                return int(self.engine_config.request_cpu[:-1]) / 1000.0
            else:
                return float(self.engine_config.request_cpu)
        except ValueError as e:
            raise DatabandConfigError(
                "failed to parse request_cpu %s: %s"
                % (self.engine_config.request_cpu, e)
            )

    def get_memory_request(self):
        try:
            return parse_kub_memory_string(self.engine_config.request_memory)
        except DatabandConfigError as e:
            raise DatabandConfigError("failed to parse request_memory: %s" % e.args)

    def check_deploy_errors(self, pod):
        pod_status = pod.status
        if pod_status.conditions:
            for condition in pod_status.conditions:
                if (
                    condition.reason == "Unschedulable"
                    and self.engine_config.check_unschedulable_condition
                ):
                    if (
                        "Insufficient cpu" in condition.message
                        or "Insufficient memory" in condition.message
                    ):
                        if self.engine_config.check_cluster_resource_capacity:
                            self.check_if_resource_request_above_max_capacity(
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

    def get_cluster_nodes(self):
        try:
            return self.kube_client.list_node(
                timeout_seconds=LIST_NODES_TIMEOUT_S
            ).items
        except Exception as ex:
            logger.warning(
                "tried to list Kubernetes cluster nodes to generate a better failure message but failed because: %s"
                % ex
            )

        return None

    def delete_pod_by_name(self, pod_name, pod_namespace):
        from kubernetes.client import V1DeleteOptions

        try:
            self.kube_client.delete_namespaced_pod(
                pod_name, pod_namespace, body=V1DeleteOptions()
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def stream_pod_logs_helper(self, pod_name, pod_namespace, log_logger, from_now):
        kwargs = {
            "name": pod_name,
            "namespace": pod_namespace,
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
            if self.engine_config.prefix_remote_log:
                # we want to remove regular header in log, and make it looks like '[pod_name] LOG FROM POD'
                prefix = "[%s]" % pod_name
                with override_log_formatting(prefix + "%(message)s"):
                    for line in logs:
                        logger.info(line[:-1].decode("utf-8"))
            else:
                for line in logs:
                    logger.info(line[:-1].decode("utf-8"))
            logger.info("end of driver logs")
        except Exception as ex:
            logger.error("Failed to stream logs for %s:  %s", pod_name, ex)

    def stream_pod_logs(self, pod, log_logger=logger, from_now=False):
        return self.stream_pod_logs_helper(
            pod_name=pod.name,
            pod_namespace=pod.namespace,
            log_logger=log_logger,
            from_now=from_now,
        )

    def wait_for_pod_started(self, launch_resp, pod, _logger=logger):
        """
        will try to raise an exception if the pod fails to start (see DbndPodLauncher.check_deploy_errors)
        """
        if launch_resp.status is None or launch_resp.status.start_time is None:
            while self.launcher.pod_not_started(pod):
                time.sleep(1)
            _logger.debug("Pod not yet started: %s", launch_resp.status)

    def end(self):
        if self.engine_config.delete_pods:
            logger.info("Deleting running pods: %s" % self.running_pods)
            for pod_id, pod_namespace in six.iteritems(self.running_pods):
                self.delete_pod_by_name(pod_id, pod_namespace)

    def get_pod_state(self, name, namespace):
        try:
            return self.launcher._task_status(self.read_pod(name, namespace))
        except Exception as e:
            logger.warning("failed to read pod state: %s" % e)
            return None

    def read_pod(self, name, namespace):
        from requests import HTTPError

        try:
            return self.kube_client.read_namespaced_pod(name, namespace)
        except HTTPError as e:
            raise DatabandRuntimeError(
                "There was an error reading the kubernetes API: {}".format(e)
            )
