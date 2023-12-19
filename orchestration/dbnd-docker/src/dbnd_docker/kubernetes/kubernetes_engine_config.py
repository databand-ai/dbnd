# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import shlex
import subprocess
import textwrap

from os import environ
from typing import Any, Dict, List, Optional

import kubernetes.client.models as k8s
import six
import yaml

from kubernetes.config import ConfigException
from six import PY2

import dbnd_docker

from dbnd import parameter
from dbnd._core.configuration.environ_config import (
    ENV_DBND__ENV_IMAGE,
    ENV_DBND__ENV_MACHINE,
    ENV_DBND__TRACKING,
    ENV_DBND_ENV,
    ENV_DBND_USER,
)
from dbnd._core.current import is_verbose
from dbnd._core.errors import DatabandConfigError
from dbnd._core.log.logging_utils import set_module_logging_to_debug
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.json_utils import dumps_safe
from dbnd._core.utils.structures import combine_mappings
from dbnd_docker.container_engine_config import ContainerEngineConfig
from dbnd_docker.docker.docker_task import DockerRunTask
from dbnd_docker.kubernetes.compat import volume_shims
from dbnd_docker.kubernetes.compat.pod_reconciler import reconcile_pods
from dbnd_docker.kubernetes.compat.secrets_shim import Secret, attach_to_pod
from dbnd_docker.kubernetes.compat.volume_shims import attach_volume_mount
from dbnd_docker.kubernetes.dns1123_clean_names import (
    clean_label_name_dns1123,
    create_pod_id,
)
from dbnd_run import errors
from dbnd_run.airflow.compat import AIRFLOW_ABOVE_10, AIRFLOW_VERSION_2
from dbnd_run.errors import executor_k8s
from dbnd_run.utils.dbnd_run_module import get_dbnd_run_conf_file
from targets import target
from targets.values import TimeDeltaValueType


logger = logging.getLogger(__name__)

ENV_DBND_POD_NAME = "DBND__POD_NAME"
ENV_DBND_POD_NAMESPACE = "DBND__POD_NAMESPACE"
ENV_DBND_DOCKER_IMAGE = "DBND__DOCKER_IMAGE"
ENV_DBND_AUTO_REMOVE_POD = "DBND__AUTO_REMOVE_POD"


class PodRetryConfiguration(object):
    RETRY_COUNT = "retry_count"
    RETRY_DELAY = "retry_delay"

    def __init__(self, exit_codes_and_reasons_to_retry_info_map):
        self.exit_codes_and_reasons_to_retry_info_map = (
            exit_codes_and_reasons_to_retry_info_map
        )

    @classmethod
    def from_kube_config(cls, kube_config):
        return cls(kube_config.pod_error_cfg_source_dict)

    def get_retry_count(self, exit_code_or_reason):
        if exit_code_or_reason not in self.exit_codes_and_reasons_to_retry_info_map:
            return None

        return self.exit_codes_and_reasons_to_retry_info_map[exit_code_or_reason][
            self.RETRY_COUNT
        ]

    def get_retry_delay(self, exit_code_or_reason):
        if exit_code_or_reason not in self.exit_codes_and_reasons_to_retry_info_map:
            return 0

        return self.exit_codes_and_reasons_to_retry_info_map[exit_code_or_reason][
            self.RETRY_DELAY
        ]

    @property
    def reasons_and_exit_codes(self):
        return self.exit_codes_and_reasons_to_retry_info_map.keys()


class KubernetesEngineConfig(ContainerEngineConfig):
    _conf__task_family = "kubernetes"

    cluster_context = parameter.none().help(
        "The Kubernetes context; you can check which context you are on by using `kubectl config get-contexts`."
    )[str]
    config_file = parameter.none().help("Custom Kubernetes config file.")[str]

    in_cluster = parameter(default=None).help(
        "Defines what Kubernetes configuration is used for the Kube client."
        "Use `false` to enforce using local credentials, or `true` to enforce the `in_cluster` mode."
        "Default: `None` (Databand will automatically decide what mode to use)."
    )[bool]

    image_pull_policy = parameter.value(
        "IfNotPresent", description="Kubernetes image_pull_policy flag"
    )

    image_pull_secrets = parameter.none().help(
        "The secret with the connection information for the `container_repository`."
    )[str]
    keep_finished_pods = parameter(default=False).help(
        "Don't delete pods on completion"
    )[bool]
    keep_failed_pods = parameter(default=False).help(
        "Don't delete failed pods. You can use it if you need to debug the system."
    )[bool]

    namespace = parameter(default="default").help(
        "The namespace in which Databand is installed inside the cluster (`databand` in this case)."
    )[str]
    secrets = parameter(empty_default=True).help(
        "User secrets to be added to every created pod"
    )[List]
    system_secrets = parameter(empty_default=True).help(
        "System secrets (used by Databand Framework)"
    )[List]
    env_vars = parameter(empty_default=True).help(
        "Assign environment variables to the pod."
    )[Dict]

    node_selectors = parameter(empty_default=True).help(
        "Assign `nodeSelector` to the pods (see [Assigning Pods to Nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/))"
    )[Dict]
    annotations = parameter(empty_default=True).help(
        "Assign annotations to the pod (see [Annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/))"
    )[Dict]
    pods_creation_batch_size = parameter.value(10)[int]
    service_account_name = parameter.none().help(
        "You need permissions to create pods for tasks, namely -  you need to have a `service_account` with the correct permissions."
    )[str]
    gcp_service_account_keys = parameter.none()[
        str
    ]  # it's actually dict, but KubeConf expects str
    affinity = parameter(empty_default=True).help(
        "Assign `affinity` to the pods (see [Assigning Pods to Nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/))"
    )[Dict]
    tolerations = parameter(empty_default=True).help(
        "Assign tolerations to the pod (see [Taints and Tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/))"
    )[List]

    hostnetwork = parameter.value(False)
    configmaps = parameter(empty_default=True)[List[str]]

    volumes = parameter.none()[List[str]]
    volume_mounts = parameter.none()[List[str]]
    security_context = parameter.none()[List[str]]
    labels = parameter.none().help(
        "Set a list of pods' labels (see [Labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/))"
    )[Dict]

    request_memory = parameter.none()[str]
    request_cpu = parameter.none()[str]
    limit_memory = parameter.none()[str]
    limit_cpu = parameter.none()[str]

    requests = parameter.none().help(
        "Setting the requests for the pod can be achieved by setting this. You can provide a standard Kubernetes Dict, however, you can also use explicit keys like `request_memory` or `request_cpu`"
    )[Dict]
    limits = parameter.none().help(
        "Setting the limits for the pod can be achieved by setting this. You can provide a standard Kubernetes Dict, however, you can also use explicit keys like `limit_memory` or `limit_cpu`"
    )[Dict]

    pod_error_cfg_source_dict = parameter(
        description='Allows flexibility of sending retry on pods that have failed with specific exit codes.  You can provide "PROCESS EXIT CODE" as a key (for example, `137`) or Kubernetes error string.'
    )[Dict]

    pod_default_retry_delay = parameter(
        description="The default amount of time to wait between retries of pods",
        default="10s",
    )[datetime.timedelta]

    submit_termination_grace_period = parameter(
        description="timedelta to let the submitted pod enter a final state"
    )[datetime.timedelta]

    startup_timeout_seconds = parameter.value(120)
    show_pod_log = parameter(default=False).help(
        "When using this engine as the task_engine, run tasks sequentially and stream their logs"
    )[bool]
    debug = parameter(default=False).help(
        "When true, displays all pod requests sent to Kubernetes and more useful debugging data."
    )[bool]
    debug_with_command = parameter(default="").help(
        "Use this command as a pod command instead of the original, can help debug complicated issues"
    )[str]
    debug_phase = parameter(default="").help(
        "Debug mode for specific phase of pod events. All these events will be printed with the full response from k8s"
    )[str]

    prefix_remote_log = parameter(default=True).help(
        "Adds [driver] or [<task_name>] prefix to logs streamed from Kubernetes to the local log"
    )
    check_unschedulable_condition = parameter(default=True).help(
        "Try to detect non-transient issues that prevent the pod from being scheduled and fail the run if needed"
    )
    check_image_pull_errors = parameter(default=True).help(
        "Try to detect image pull issues that prevent the pod from being scheduled and fail the run if needed"
    )
    check_running_pod_errors = parameter(default=False).help(
        "Try to detect running pod issues like failed ContainersReady condition (pod is deleted)"
    )
    check_cluster_resource_capacity = parameter(default=True).help(
        "When a pod can't be scheduled due to CPU or memory constraints, check if the constraints are possible to satisfy in the cluster"
    )

    startup_timeout = parameter(default="10m").help(
        "Time to wait for the pod to get into the Running state"
    )[datetime.timedelta]

    max_retries_on_log_stream_failure = parameter(default=50).help(
        "Determines maximum retry attempts while waiting for pod completion and streaming logs in --interactive mode. If set to 0 - no retries will be performed."
    )[int]

    dashboard_url = parameter(default=None).help(
        "skeleton url to display as kubernetes dashboard"
    )[str]

    pod_log_url = parameter(default=None).help("skeleton url to display logs of pods")[
        str
    ]

    pod_yaml = parameter(default=get_dbnd_run_conf_file("kubernetes-pod.yaml")).help(
        "Base YAML to use to run databand task/driver"
    )[str]

    trap_exit_file_flag = parameter(default=None).help("trap exit file")[str]
    auto_remove = parameter(
        default=False,
        description="Auto-removal of the pod when the container has finished.",
    )[bool]
    detach_run = parameter(
        default=False, description="Submit run only, do not wait for its completion."
    )[bool]

    watcher_request_timeout_seconds = parameter(
        default=300,
        description="How many seconds watcher should wait "
        "for events "
        "until timeout",
    )[int]

    watcher_recreation_interval_seconds = parameter(
        default=30,
        description="How many seconds to wait before resurrecting watcher after the timeout",
    )[int]

    watcher_client_timeout_seconds = parameter(
        default=50,
        description="How many seconds to wait before timeout occurs in watcher on client side (read)",
    )[int]

    log_pod_events_on_sigterm = parameter(
        default=False,
        description="When receiving sigterm log the current pod state to debug why the pod was terminated",
    )

    pending_zombies_timeout = parameter(
        default="5h",
        description="Amount of time we will wait before a pending pod would consider a"
        " zombie and we will set it to fail",
    ).type(TimeDeltaValueType)

    zombie_query_interval_secs = parameter(
        default=600,
        description="Amount of seconds we wait between zombie checking intervals. "
        "Default: 600 sec => 10 minutes",
    )

    zombie_threshold_secs = parameter(
        default=300,
        description="If the job has not heartbeat in this many seconds, "
        "the scheduler will mark the associated task instance as failed and will re-schedule the task.",
    )

    # airflow live logs feature
    # ------------------------- #
    airflow_log_enabled = parameter(
        default=False,
        description="Enables Airflow Live log at KubernetesExecutor feature",
    )[bool]
    airflow_log_image = parameter(
        default=None,
        description="Override the image that will be used to add sidecar to the run which will expose the live "
        "logs of the run. By default, the main container image will be used",
    )[str]
    airflow_log_folder = parameter(
        default="/usr/local/airflow/logs",
        description="Specify the location on the airflow image (sidecar), where we mount the logs from the original"
        " container and expose them to airflow UI.",
    )[str]
    airflow_log_port = parameter(
        default="8793",
        description="The port airflow live log sidecar will expose its service. This port should match the port "
        "airflow webserver tries to access the live logs ",
    )[str]

    airflow_log_trap_exit_flag_default = parameter(
        default="/tmp/pod/terminated",
        description="The path that will be used by default if `airflow_log_enabled` is true ",
    )[str]

    container_airflow_log_path = parameter(
        default="/root/airflow/logs/",
        description="The path to the airflow logs, on the databand container.",
    )
    host_as_ip_for_live_logs = parameter(
        default=True,
        description="Set the host of the pod to be the IP address of the pod. "
        "In Kubernetes normally, only Services get DNS names, not Pods. "
        "We use the IP for airflow webserver to lookup the sidecar See more here: https://stackoverflow.com/a/59262628",
    )

    fix_pickle = parameter(default=False)

    def _initialize(self):
        super(KubernetesEngineConfig, self)._initialize()

        if self.debug:
            logger.warning(
                "Running in debug mode, setting all k8s loggers to debug, waiting for every pod completion!"
            )
            if AIRFLOW_VERSION_2:
                from airflow import kubernetes
            else:
                from airflow.contrib import kubernetes

            set_module_logging_to_debug([dbnd_docker, kubernetes])
            self.detach_run = False
        if self.show_pod_log:
            logger.warning(
                "Showing pod logs at runtime, waiting for every pod completion!"
            )
            self.detach_run = False
        if self.auto_remove and not self.detach_run:
            logger.warning(
                "Can't auto remove pod if not running from detach_run=True mode, "
                "switching to auto_remove=False"
            )
            self.auto_remove = False

        if (
            self.airflow_log_enabled
            and not self.trap_exit_file_flag
            and self.airflow_log_trap_exit_flag_default
        ):
            self.trap_exit_file_flag = self.airflow_log_trap_exit_flag_default

        self.pod_retry_config = PodRetryConfiguration.from_kube_config(self)

    def get_docker_ctrl(self, task_run):
        from dbnd_docker.kubernetes.kubernetes_task_run_ctrl import (
            KubernetesTaskRunCtrl,
        )

        return KubernetesTaskRunCtrl(task_run=task_run)

    def submit_to_engine_task(self, env, task_name, args, interactive=True):
        docker_engine = self
        if not interactive:
            docker_engine = docker_engine.clone(auto_remove=True, detach_run=True)
        return DockerRunTask(
            task_name=task_name,
            command=subprocess.list2cmdline(args),
            image=self.full_image,
            docker_engine=docker_engine,
            task_is_system=True,
        )

    def cleanup_after_run(self):
        # this run was submitted by task_run_async - we need to cleanup ourself
        if not environ_enabled(ENV_DBND_AUTO_REMOVE_POD):
            return
        if ENV_DBND_POD_NAME in environ and ENV_DBND_POD_NAMESPACE in environ:
            try:
                logger.warning(
                    "Auto deleteing pod as accordingly to '%s' env variable"
                    % ENV_DBND_AUTO_REMOVE_POD
                )
                kube_dbnd = self.build_kube_dbnd()
                kube_dbnd.delete_pod(
                    name=environ[ENV_DBND_POD_NAME],
                    namespace=environ[ENV_DBND_POD_NAMESPACE],
                )
            except Exception as e:
                logger.warning("Tried to delete this pod but failed: %s" % e)
        else:
            logger.warning(
                "Auto deleting pod as set, but pod name and pod namespace is not defined"
            )

    def get_dashboard_link(self, pod_namespace: str, pod_name: str) -> Optional[str]:
        if not self.dashboard_url:
            return None
        try:
            return self.dashboard_url.format(namespace=pod_namespace, pod=pod_name)
        except Exception:
            logger.exception(
                "Failed to generate dashboard url from %s" % self.dashboard_url
            )
        return None

    def get_pod_log_link(self, pod_namespace: str, pod_name: str) -> Optional[str]:
        if not self.pod_log_url:
            return None
        try:
            return self.pod_log_url.format(
                namespace=pod_namespace,
                pod=pod_name,
                timestamp=datetime.datetime.now().isoformat(),
            )
        except Exception:
            logger.exception("Internal error on generating pod log url")
        return None

    def get_kube_client(self):
        from kubernetes import client, config

        # if in_cluster is set to None, we set it dynamically by trying to set
        # the k8s's config as if we are in a cluster, and if it fails, we set it
        # as we are not running in a cluster.
        if self.in_cluster is None:
            try:
                config.load_incluster_config()
                self.in_cluster = True
            except ConfigException:
                try:
                    config.load_kube_config(
                        config_file=self.config_file, context=self.cluster_context
                    )
                except ConfigException as e:
                    raise executor_k8s.failed_to_load_config_file(e)
                self.in_cluster = False
        else:
            try:
                if self.in_cluster:
                    config.load_incluster_config()
                else:
                    config.load_kube_config(
                        config_file=self.config_file, context=self.cluster_context
                    )
            except ConfigException as e:
                raise errors.executor_k8s.failed_to_connect_to_cluster(
                    self.in_cluster, e
                )

        if PY2:
            # For connect_get_namespaced_pod_exec
            from kubernetes.client import Configuration

            configuration = Configuration()
            configuration.assert_hostname = False
            Configuration.set_default(configuration)
        return client.CoreV1Api()

    def build_kube_dbnd(self):
        from dbnd_docker.kubernetes.kube_dbnd_client import DbndKubernetesClient

        kube_client = self.get_kube_client()
        kube_dbnd = DbndKubernetesClient(kube_client=kube_client, engine_config=self)
        return kube_dbnd

    def get_pod_name(self, task_run, try_number):
        pod_name = create_pod_id(task_run)
        if try_number is not None:
            pod_name = "%s-%s" % (pod_name, try_number)
        return pod_name

    def build_pod(
        self,
        task_run: TaskRun,
        cmds: List[str],
        args: Optional[List[str]] = None,
        labels: Optional[Dict[str, str]] = None,
        try_number: Optional[int] = None,
        include_system_secrets: bool = False,
    ) -> k8s.V1Pod:
        if not self.container_tag:
            raise DatabandConfigError(
                "Your container tag is None, please check your configuration",
                help_msg="Container tag should be assigned",
            )

        pod_name = self.get_pod_name(task_run=task_run, try_number=try_number)

        image = self.full_image
        labels = combine_mappings(labels, self.labels)
        labels["pod_name"] = pod_name

        labels["dbnd_run_uid"] = task_run.run.run_uid
        labels["dbnd_task_run_uid"] = task_run.task_run_uid
        labels["dbnd_task_run_attempt_uid"] = task_run.task_run_attempt_uid
        labels[
            "dbnd_task_family"
        ] = task_run.task.task_definition.full_task_family_short
        labels["dbnd_task_name"] = task_run.task.task_name
        labels["dbnd_task_af_id"] = task_run.task_af_id

        # for easier pod deletion (kubectl delete pod -l dbnd=task_run_executor -n <my_namespace>)
        if task_run.task.task_is_system:
            labels["dbnd"] = "dbnd_system_task_run"
        else:
            labels["dbnd"] = "task_run_executor"

        # we need to be sure that the values meet the dns label names RFC
        # https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
        labels = {
            label_name: clean_label_name_dns1123(str(label_value))
            for label_name, label_value in six.iteritems(labels)
        }
        if is_verbose():
            logger.info("Build pod with kubernetes labels {}".format(labels))

        annotations = self.annotations.copy()
        if self.gcp_service_account_keys:
            annotations[
                "iam.cloud.google.com/service-account"
            ] = self.gcp_service_account_keys
        annotations["dbnd_tracker"] = task_run.task_tracker_url

        from dbnd_docker.kubernetes.vendorized_airflow.dbnd_extended_resources import (
            DbndExtendedResources,
        )

        resources = DbndExtendedResources(
            requests=self.requests,
            limits=self.limits,
            request_memory=self.request_memory,
            request_cpu=self.request_cpu,
            limit_memory=self.limit_memory,
            limit_cpu=self.limit_cpu,
        )
        env_vars = {
            ENV_DBND_POD_NAME: pod_name,
            ENV_DBND_POD_NAMESPACE: self.namespace,
            ENV_DBND_USER: task_run.task_run_env.user,
            ENV_DBND__ENV_IMAGE: image,
            ENV_DBND_ENV: task_run.run.run_executor.env.task_name,
            ENV_DBND__ENV_MACHINE: "%s at %s" % (pod_name, self.namespace),
        }

        if AIRFLOW_VERSION_2:
            env_vars[
                "AIRFLOW__CORE__TASK_RUNNER"
            ] = "dbnd_run.airflow.compat.dbnd_task_runner.DbndStandardTaskRunner"

        if self.auto_remove:
            env_vars[ENV_DBND_AUTO_REMOVE_POD] = "True"
        env_vars[self._params.get_param_env_key(self, "in_cluster")] = "True"
        env_vars["AIRFLOW__KUBERNETES__IN_CLUSTER"] = "True"
        env_vars[
            "DBND__RUN_INFO__SOURCE_VERSION"
        ] = task_run.run.context.task_run_env.user_code_version
        env_vars["AIRFLOW__KUBERNETES__DAGS_IN_IMAGE"] = "True"

        # let's explicitly disable tracking
        env_vars[ENV_DBND__TRACKING] = "False"
        # we want that all next runs will be able to use the image that we have in our configuration

        env_vars.update(
            self._params.to_env_map(self, "container_repository", "container_tag")
        )

        env_vars.update(self.env_vars)
        env_vars.update(task_run.run.run_executor.get_context_spawn_env())

        secrets = self.get_secrets(include_system_secrets=include_system_secrets)

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

        if self.debug_with_command:
            logger.warning(
                "%s replacing pod %s command with '%s', original command=`%s`",
                task_run,
                pod_name,
                self.debug_with_command,
                subprocess.list2cmdline(cmds),
            )
            cmds = shlex.split(self.debug_with_command)

        base_pod = self._build_base_pod()

        pod = self._to_real_pod(
            cmds=cmds,
            args=args,
            namespace=self.namespace,
            name=pod_name,
            envs=env_vars,
            image=image,
            labels=labels,
            secrets=secrets,
            resources=resources,
            annotations=annotations,
        )

        final_pod = reconcile_pods(base_pod, pod)

        return final_pod

    def _to_real_pod(
        self,
        cmds: List[str],
        args: List[str],
        namespace: str,
        name: str,
        image: str,
        envs: Dict[str, str],
        labels: Dict[str, str],
        annotations: Dict[str, str],
        resources: "DbndExtendedResources",
        secrets: List["Secret"],
    ) -> k8s.V1Pod:

        # TODO add yaml template as basis
        BASE_CONTAINER_NAME = "base"
        kc: KubernetesEngineConfig = self
        meta = k8s.V1ObjectMeta(
            labels=labels, name=name, namespace=namespace, annotations=annotations
        )
        if kc.image_pull_secrets:
            image_pull_secrets = [
                k8s.V1LocalObjectReference(i) for i in kc.image_pull_secrets.split(",")
            ]
        else:
            image_pull_secrets = []
        spec = k8s.V1PodSpec(
            # init_containers=kc.init_containers,
            containers=[
                k8s.V1Container(
                    image=image,
                    command=cmds,
                    env_from=[],
                    name=BASE_CONTAINER_NAME,
                    env=[
                        k8s.V1EnvVar(name=key, value=val) for key, val in envs.items()
                    ],
                    args=args,
                    image_pull_policy=kc.image_pull_policy,
                )
            ],
            image_pull_secrets=image_pull_secrets,
            service_account_name=kc.service_account_name,
            node_selector=kc.node_selectors,
            # dns_policy=kc.dnspolicy,
            host_network=kc.hostnetwork,
            tolerations=kc.tolerations,
            affinity=kc.affinity,
            security_context=kc.security_context,
        )

        k8_pod = k8s.V1Pod(spec=spec, metadata=meta)
        for configmap_name in kc.configmaps:
            env_var = k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap_name)
            )
            k8_pod.spec.containers[0].env_from.append(env_var)

        volumes = kc.volumes or []
        for volume in volumes:
            k8_pod = volume_shims.attach_to_pod(k8_pod, volume)

        mounts = kc.volume_mounts or []
        for volume_mount in mounts:
            k8_pod = attach_volume_mount(k8_pod, volume_mount)

        secret: Secret
        for secret in secrets:
            if AIRFLOW_ABOVE_10:
                k8_pod = secret.attach_to_pod(k8_pod)
            else:
                k8_pod = attach_to_pod(secret, k8_pod)

        k8_pod = resources.attach_to_pod(k8_pod)
        return k8_pod

    def _build_base_pod(self) -> k8s.V1Pod:
        from kubernetes.client import ApiClient

        basis_pod_yaml = target(self.pod_yaml).read()
        basis_pod_dict = yaml.safe_load(basis_pod_yaml) or {}
        api_client = ApiClient()
        return api_client._ApiClient__deserialize_model(basis_pod_dict, k8s.V1Pod)

    def get_secrets(self, include_system_secrets=True) -> List["Secret"]:
        """Defines any necessary secrets for the pod executor"""

        result = []
        if include_system_secrets:
            secrets = self.system_secrets + self.secrets
        else:
            secrets = self.secrets
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

    def build_kube_pod_req(self, pod: k8s.V1Pod) -> Dict[str, Any]:
        from kubernetes.client import ApiClient

        return ApiClient().sanitize_for_serialization(pod)

    # TODO: [#2] add them in-place?
    def apply_env_vars_to_pod(self, pod):
        pod.envs["AIRFLOW__KUBERNETES__DAGS_IN_IMAGE"] = "True"
        pod.envs[ENV_DBND__TRACKING] = "False"


def readable_pod_request(pod_req):
    try:
        return yaml.dump(
            pod_req,
            encoding="utf-8",
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        ).decode("ascii", errors="ignore")
    except Exception as ex:
        logger.info("Failed to create readable pod request representation: %s", ex)
        return dumps_safe(pod_req)
