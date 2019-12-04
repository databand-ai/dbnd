import datetime
import logging
import math
import subprocess
import textwrap

from os import environ
from typing import Dict, List, Optional

import airflow.contrib.kubernetes.pod

from six import PY2

import dbnd_docker

from dbnd import parameter
from dbnd._core.configuration.environ_config import ENV_DBND_USER
from dbnd._core.errors import DatabandConfigError
from dbnd._core.log.logging_utils import set_module_logging_to_debug
from dbnd._core.settings.engine import ContainerEngineConfig
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.git import GIT_ENV
from dbnd._core.utils.string_utils import clean_job_name_dns1123
from dbnd._core.utils.structures import combine_mappings
from dbnd_airflow.executors import AirflowTaskExecutorType
from targets import target
from targets.values.version_value import get_project_git


SI_MEMORY_SUFFIXES = ["K", "M", "G", "T", "P", "E"]
ISO_MEMORY_SUFFIXES = ["%si" % s for s in SI_MEMORY_SUFFIXES]
MEMORY_SUFFIXES_WITH_BASE_AND_POWER = [
    (ISO_MEMORY_SUFFIXES, 2, 10),
    (SI_MEMORY_SUFFIXES, 10, 3),
]

logger = logging.getLogger(__name__)

ENV_DBND_POD_NAME = "DBND__POD_NAME"
ENV_DBND_POD_NAMESPACE = "DBND__POD_NAMESPACE"


class KubernetesEngineConfig(ContainerEngineConfig):
    _conf__task_family = "kubernetes"

    parallel = True
    task_executor_type = AirflowTaskExecutorType.airflow_kubernetes

    use_airflow_executor = parameter.help("Submit Pod via Airflow Executor").value(
        False
    )

    cluster_context = parameter.none().help("Kubernetes cluster context")[str]
    config_file = parameter.none().help("Custom Kubernetes config file")[str]

    in_cluster = parameter(default=False)[bool]

    image_pull_policy = parameter.value(
        "IfNotPresent", description="Kubernetes image_pull_policy flag"
    )

    image_pull_secrets = parameter.none().help("Secret to use for image pull")[str]
    delete_pods = parameter.none().help("Delete pods once completion")[bool]
    keep_failed_pods = parameter(default=True).help(
        "Don't delete failed pods, even if the delete_pods flag is true"
    )[bool]

    namespace = parameter(default="default")[str]
    secrets = parameter(empty_default=True).help(
        "User secrets to be added to every created pod"
    )[List]
    system_secrets = parameter(empty_default=True).help(
        "System secrets (used by Databand Framework)"
    )[List]
    env_vars = parameter(empty_default=True)[Dict]

    node_selectors = parameter(empty_default=True)[Dict]
    annotations = parameter(empty_default=True)[Dict]
    pods_creation_batch_size = parameter.value(10)[int]
    service_account_name = parameter.none()[str]
    gcp_service_account_keys = parameter.none()[
        str
    ]  # it's actually dict, but KubeConf expects str
    affinity = parameter(empty_default=True)[Dict]
    tolerations = parameter(empty_default=True)[List]

    hostnetwork = parameter.value(False)
    configmaps = parameter(empty_default=True)[List[str]]

    volumes = parameter.none()[List[str]]
    volume_mounts = parameter.none()[List[str]]
    security_context = parameter.none()[List[str]]
    labels = parameter.none()[Dict]

    request_memory = parameter.none()[str]
    request_cpu = parameter.none()[str]
    limit_memory = parameter.none()[str]
    limit_cpu = parameter.none()[str]

    requests = parameter.none()[Dict]
    limits = parameter.none()[Dict]

    startup_timeout_seconds = parameter.value(120)
    show_pod_log = parameter(default=False).help(
        "When using this engine as the task_engine, run tasks sequentially and stream their logs"
    )[bool]
    debug = parameter(default=False).help(
        "Equalent to show_pod_log=True + show all debug information"
    )[bool]

    prefix_remote_log = parameter(default=True).help(
        "Adds [driver] or [<task_name>] prefix to logs streamed from Kubernetes to the local log"
    )
    check_unschedulable_condition = parameter(default=True).help(
        "Try to detect non-transient issues that prevent the pod from being scheduled and fail the run if needed"
    )
    check_cluster_resource_capacity = parameter(default=True).help(
        "When a pod can't be scheduled due to cpu or memory constraints, check if the constraints are possible to satisfy in the cluster"
    )

    task_run_async = parameter(default=False).help(
        "When using this engine to run the task driver - exit the local dbnd process when the driver is launched instead of waiting for completion"
    )

    dashboard_url = parameter(default=None).help(
        "skeleton url to display as kubernetes dashboard"
    )[str]

    pod_log_url = parameter(default=None).help("skeleton url to display logs of pods")[
        str
    ]

    pod_yaml = parameter(default="${DBND_LIB}/conf/kubernetes-pod.yaml").help(
        "Base yaml to use to run databand task/driver"
    )[str]

    trap_exit_file_flag = parameter(default=None).help("trap exit file")[str]

    def will_submit_by_executor(self):
        return self.task_executor_type == AirflowTaskExecutorType.airflow_kubernetes

    def _initialize(self):
        super(KubernetesEngineConfig, self)._initialize()

        if self.debug:
            logger.info("Running in debug mode, setting all k8s loggers to debug")
            import airflow.contrib.kubernetes

            set_module_logging_to_debug([dbnd_docker, airflow.contrib.kubernetes])

    def get_docker_ctrl(self, task_run):
        from dbnd_docker.kubernetes.kubernetes_task_run_ctrl import (
            KubernetesTaskRunCtrl,
        )

        return KubernetesTaskRunCtrl(task_run=task_run)

    def cleanup_after_run(self):
        if not self.task_run_async or not self.delete_pods:
            return

        from kubernetes.client import V1DeleteOptions

        # we know where we run, we can kill ourself
        if ENV_DBND_POD_NAME in environ and ENV_DBND_POD_NAMESPACE in environ:
            try:
                client = self.get_kube_client(in_cluster=True)
                client.delete_namespaced_pod(
                    environ[ENV_DBND_POD_NAME],
                    environ[ENV_DBND_POD_NAMESPACE],
                    body=V1DeleteOptions(),
                )
            except Exception as e:
                logger.warning("Tried to delete this pod but failed: %s" % e)

    def get_dashboard_link(self, pod):
        if not self.dashboard_url:
            return None
        try:
            return self.dashboard_url.format(namespace=pod.namespace, pod=pod.name)
        except Exception:
            logger.exception(
                "Failed to generate dashboard url from %s" % self.dashboard_url
            )
        return None

    def get_pod_log_link(self, pod):
        if not self.pod_log_url:
            return None
        try:
            return self.pod_log_url.format(
                namespace=pod.namespace,
                pod=pod.name,
                timestamp=datetime.datetime.now().isoformat(),
            )
        except Exception:
            logger.exception("Internal error on generating pod log url")
        return None

    def get_kube_client(self, in_cluster=None):
        from kubernetes import config, client

        if in_cluster is None:
            in_cluster = self.in_cluster
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config(
                config_file=self.config_file, context=self.cluster_context
            )

        if PY2:
            # For connect_get_namespaced_pod_exec
            from kubernetes.client import Configuration

            configuration = Configuration()
            configuration.assert_hostname = False
            Configuration.set_default(configuration)
        return client.CoreV1Api()

    def build_kube_dbnd(self, in_cluster=None):
        from dbnd_docker.kubernetes.kube_dbnd_client import DbndKubernetesClient

        kube_client = self.get_kube_client(in_cluster=in_cluster)

        kube_dbnd = DbndKubernetesClient(kube_client=kube_client, engine_config=self)
        if self.debug:
            kube_dbnd.launcher.log.setLevel(logging.DEBUG)
        return kube_dbnd

    def build_pod(self, task_run, cmds, args=None, labels=None):
        # type: (TaskRun, List[str], Optional[List[str]], Optional[List[str]]) ->Pod
        pod_name = task_run.job_id__dns1123

        labels = combine_mappings(labels, self.labels)
        labels["dbnd_run_uid"] = clean_job_name_dns1123(str(task_run.run.run_uid))
        labels["dbnd_task_run_uid"] = clean_job_name_dns1123(str(task_run.task_run_uid))

        annotations = self.annotations.copy()
        if self.gcp_service_account_keys:
            annotations[
                "iam.cloud.google.com/service-account"
            ] = self.gcp_service_account_keys
        annotations["dbnd_tracker"] = task_run.task_tracker_url

        resources = DbndExtendedResources(
            requests=self.requests,
            limits=self.limits,
            request_memory=self.request_memory,
            request_cpu=self.request_cpu,
            limit_memory=self.limit_memory,
            limit_cpu=self.limit_cpu,
        )
        env_vars = combine_mappings(
            {
                ENV_DBND_POD_NAME: pod_name,
                ENV_DBND_POD_NAMESPACE: self.namespace,
                ENV_DBND_USER: task_run.task_run_env.user,
            },
            self.env_vars,
        )
        env_vars.update(
            self._params.to_env_map("container_repository", "container_tag")
        )
        env_vars[self._params.get_param_env_key("in_cluster")] = "True"
        env_vars["AIRFLOW__KUBERNETES__IN_CLUSTER"] = "True"
        env_vars[GIT_ENV] = get_project_git()  # git repo not packaged in docker image
        env_vars.update(task_run.run.get_context_spawn_env())

        secrets = self.get_secrets()

        from airflow.contrib.kubernetes.pod import Pod

        if self.trap_exit_file_flag:
            args = [
                textwrap.dedent(
                    """
                trap "touch {trap_file}" EXIT
                {command}
                """.format(
                        trap_file=self.trap_exit_file_flag,
                        command=subprocess.list2cmdline(cmds),
                    )
                )
            ]
            # we update cmd now
            cmds = ["/bin/bash", "-c"]

        if not self.container_tag:
            raise Exception(
                "Your container tag is None, please check your configuration"
            )

        pod = Pod(
            namespace=self.namespace,
            name=pod_name,
            envs=env_vars,
            image="{}:{}".format(self.container_repository, self.container_tag),
            cmds=cmds,
            args=args,
            labels=labels,
            image_pull_policy=self.image_pull_policy,
            image_pull_secrets=self.image_pull_secrets,
            secrets=secrets,
            service_account_name=self.service_account_name,
            volumes=self.volumes,
            volume_mounts=self.volume_mounts,
            annotations=annotations,
            node_selectors=self.node_selectors,
            affinity=self.affinity,
            tolerations=self.tolerations,
            security_context=self.security_context,
            configmaps=self.configmaps,
            hostnetwork=self.hostnetwork,
            resources=resources,
        )

        if self.pod_yaml:
            pod.pod_yaml = target(self.pod_yaml).read()

        return pod

    def get_secrets(self):
        """Defines any necessary secrets for the pod executor"""
        from airflow.contrib.kubernetes.secret import Secret

        result = []
        secrets = self.system_secrets + self.secrets
        for secret_data in secrets:
            result.append(
                Secret(
                    deploy_type=secret_data.get("type"),
                    deploy_target=secret_data.get("target"),
                    secret=secret_data.get("secret"),
                    key=secret_data.get("key"),
                )
            )

        return result


def parse_kub_memory_string(memory_string):
    """
    https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-memory
    :return: float value of requested bytes or None
    """
    if not memory_string:
        return None

    try:
        for suffixes, base, power in MEMORY_SUFFIXES_WITH_BASE_AND_POWER:
            for i, s in enumerate(suffixes, start=1):
                if memory_string.endswith(s) or memory_string.endswith(s.lower()):
                    return float(memory_string[: -len(s)]) * math.pow(base, power * i)
    except ValueError as e:
        raise DatabandConfigError("memory parse failed for %s: %s" % (memory_string, e))

    raise DatabandConfigError(
        "memory parse failed for %s: suffix not recognized" % memory_string
    )


class DbndExtendedResources(airflow.contrib.kubernetes.pod.Resources):
    def __init__(self, requests=None, limits=None, **kwargs):
        # py2 support: Resources is not object
        airflow.contrib.kubernetes.pod.Resources.__init__(self, **kwargs)
        self.requests = requests
        self.limits = limits
