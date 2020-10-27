import datetime
import logging
import subprocess
import textwrap

from os import environ
from typing import Dict, List, Optional

import yaml

from six import PY2

import dbnd_docker

from dbnd import parameter
from dbnd._core.configuration.environ_config import (
    ENV_DBND__ENV_IMAGE,
    ENV_DBND__ENV_MACHINE,
    ENV_DBND_ENV,
    ENV_DBND_USER,
)
from dbnd._core.errors import DatabandConfigError, friendly_error
from dbnd._core.log.logging_utils import set_module_logging_to_debug
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.json_utils import dumps_safe
from dbnd._core.utils.string_utils import clean_job_name_dns1123
from dbnd._core.utils.structures import combine_mappings
from dbnd_docker.container_engine_config import ContainerEngineConfig
from dbnd_docker.docker.docker_task import DockerRunTask
from targets import target


logger = logging.getLogger(__name__)

ENV_DBND_POD_NAME = "DBND__POD_NAME"
ENV_DBND_POD_NAMESPACE = "DBND__POD_NAMESPACE"
ENV_DBND_DOCKER_IMAGE = "DBND__DOCKER_IMAGE"
ENV_DBND_AUTO_REMOVE_POD = "DBND__AUTO_REMOVE_POD"


class KubernetesEngineConfig(ContainerEngineConfig):
    _conf__task_family = "kubernetes"

    cluster_context = parameter.none().help("Kubernetes cluster context")[str]
    config_file = parameter.none().help("Custom Kubernetes config file")[str]

    in_cluster = parameter(default=False)[bool]

    image_pull_policy = parameter.value(
        "IfNotPresent", description="Kubernetes image_pull_policy flag"
    )

    image_pull_secrets = parameter.none().help("Secret to use for image pull")[str]
    keep_finished_pods = parameter(default=False).help(
        "Don't delete pods on completion"
    )[bool]
    keep_failed_pods = parameter(default=False).help("Don't delete failed pods")[bool]

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

    pod_exit_code_to_retry_count = parameter(empty_default=True).help(
        "Mapping between pod exit code to amount of pod retry attempts"
    )[Dict]
    pod_retry_delay = parameter.help(
        "The delay between each pod retry attempt in time delta format. 1m, 5s, 1h, etc."
    )[datetime.timedelta]
    retry_on_image_pull_error_count = parameter.help(
        "Describes the amount of retry attempts when a pod fails with " "'ErrImagePull'"
    ).default(0)[int]

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

    startup_timeout = parameter(default="10m").help(
        "Time to wait for pod getting into Running state"
    )[datetime.timedelta]

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
    auto_remove = parameter(
        default=False,
        description="Auto-removal of the pod when container has finished.",
    )[bool]
    detach_run = parameter(
        default=False, description="Submit run only, do not wait for it completion."
    )[bool]

    submit_termination_grace_period = parameter(
        description="timedelta to let the submitted pod enter a final state"
    )[datetime.timedelta]

    watcher_request_timeout_seconds = parameter(
        default=300,
        description="How many seconds watcher should wait "
        "for events "
        "until timeout",
    )[int]

    watcher_recreation_interval_seconds = parameter(
        default=30,
        description="How many seconds to wait before resurrecting watcher after it dies",
    )[int]

    watcher_client_timeout_seconds = parameter(
        default=50,
        description="How many seconds to wait before timeout occurs in watcher on client side (read)",
    )[int]

    log_pod_events_on_sigterm = parameter(
        default=False,
        description="When receiving sigterm log current pod state to debug why the pod was terminated",
    )

    def _initialize(self):
        super(KubernetesEngineConfig, self)._initialize()

        if self.debug:
            logger.warning(
                "Running in debug mode, setting all k8s loggers to debug, waiting for every pod completion!"
            )
            import airflow.contrib.kubernetes

            set_module_logging_to_debug([dbnd_docker, airflow.contrib.kubernetes])
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
        from kubernetes.config import ConfigException

        try:
            kube_client = self.get_kube_client(in_cluster=in_cluster)
        except ConfigException as e:
            raise friendly_error.executor_k8s.failed_to_connect_to_cluster(
                self.in_cluster, e
            )

        kube_dbnd = DbndKubernetesClient(kube_client=kube_client, engine_config=self)
        return kube_dbnd

    def get_pod_name(self, task_run, try_number):
        pod_name = task_run.job_id__dns1123
        if try_number is not None:
            pod_name = "%s-%s" % (pod_name, try_number)
        return pod_name

    def build_pod(
        self,
        task_run,
        cmds,
        args=None,
        labels=None,
        try_number=None,
        include_system_secrets=False,
    ):
        # type: (TaskRun, List[str], Optional[List[str]], Optional[Dict[str,str]], Optional[int]) ->Pod
        pod_name = self.get_pod_name(task_run=task_run, try_number=try_number)

        image = self.full_image
        labels = combine_mappings(labels, self.labels)
        labels["dbnd_run_uid"] = clean_job_name_dns1123(str(task_run.run.run_uid))
        labels["dbnd_task_run_uid"] = clean_job_name_dns1123(str(task_run.task_run_uid))
        labels[
            "dbnd"
        ] = "task_run"  # for easier pod deletion (kubectl delete pod -l dbnd=task_run -n <my_namespace>)

        if task_run.task.task_is_system:
            labels["dbnd"] = "dbnd_system_task_run"
        annotations = self.annotations.copy()
        if self.gcp_service_account_keys:
            annotations[
                "iam.cloud.google.com/service-account"
            ] = self.gcp_service_account_keys
        annotations["dbnd_tracker"] = task_run.task_tracker_url

        from dbnd_docker.kubernetes.dbnd_extended_resources import DbndExtendedResources

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
            ENV_DBND_ENV: task_run.run.env.task_name,
            ENV_DBND__ENV_MACHINE: "%s at %s" % (pod_name, self.namespace),
        }
        if self.auto_remove:
            env_vars[ENV_DBND_AUTO_REMOVE_POD] = "True"
        env_vars[self._params.get_param_env_key("in_cluster")] = "True"
        env_vars["AIRFLOW__KUBERNETES__IN_CLUSTER"] = "True"
        env_vars[
            "DBND__RUN_INFO__SOURCE_VERSION"
        ] = task_run.run.context.task_run_env.user_code_version

        # we want that all next runs will be able to use the image that we have in our configuration

        env_vars.update(
            self._params.to_env_map("container_repository", "container_tag")
        )

        env_vars.update(self.env_vars)
        env_vars.update(task_run.run.get_context_spawn_env())

        secrets = self.get_secrets(include_system_secrets=include_system_secrets)

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
            raise DatabandConfigError(
                "Your container tag is None, please check your configuration",
                help_msg="Container tag should be assigned",
            )

        pod = Pod(
            namespace=self.namespace,
            name=pod_name,
            envs=env_vars,
            image=image,
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

    def get_secrets(self, include_system_secrets=True):
        """Defines any necessary secrets for the pod executor"""
        from airflow.contrib.kubernetes.secret import Secret

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

    def build_kube_pod_req(self, pod):
        from dbnd_airflow.airflow_extensions.request_factory import (
            DbndPodRequestFactory,
        )

        self.apply_env_vars_to_pod(pod)
        kube_req_factory = DbndPodRequestFactory()
        if hasattr(pod, "pod_yaml"):
            kube_req_factory._yaml = pod.pod_yaml

        req = kube_req_factory.create(pod)
        return req

    def apply_env_vars_to_pod(self, pod):
        pod.envs["AIRFLOW__KUBERNETES__DAGS_IN_IMAGE"] = "True"


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
