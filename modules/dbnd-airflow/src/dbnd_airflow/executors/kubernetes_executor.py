import logging
import multiprocessing

from queue import Empty

from airflow.contrib.executors.kubernetes_executor import (
    AirflowKubernetesScheduler,
    KubeConfig,
    KubernetesExecutor,
    KubernetesJobWatcher,
)
from airflow.contrib.kubernetes.pod_launcher import PodLauncher
from airflow.models import KubeWorkerIdentifier
from airflow.utils.db import provide_session
from airflow.utils.state import State

from dbnd._core.current import try_get_databand_run
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd_airflow.airflow_extensions.request_factory import DbndPodRequestFactory
from dbnd_docker.kubernetes.kube_dbnd_client import DbndKubernetesClient
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig
from kubernetes.client.rest import ApiException


MAX_POD_ID_LEN = 253

logger = logging.getLogger(__name__)


def _update_airflow_kube_config(airflow_kube_config, engine_config):
    # type:( KubeConfig, KubernetesEngineConfig) -> None
    # We (almost) don't need this mapping any more
    # Pod is created using  databand KubeConfig
    # We still are mapping databand KubeConfig -> airflow KubeConfig as some functions are using values from it.
    ec = engine_config

    secrets = ec.get_secrets()
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

    if ec.delete_pods is not None:
        airflow_kube_config.delete_worker_pods = ec.delete_pods
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

        # PATCH manage watcher
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.current_resource_version = 0
        self.kube_watcher = self._make_kube_watcher_dbnd()

        # PATCH use dbnd client
        self.kube_dbnd = kube_dbnd
        self.launcher = self.kube_dbnd.launcher

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
            self.namespace,
            self.watcher_queue,
            self.current_resource_version,
            self.worker_uuid,
            self.kube_config,
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
        with task_run.runner.task_run_driver_context():
            kubernetes_config = build_task_from_config(
                task_name=self.kube_dbnd.engine_config.task_name
            )  # type: KubernetesEngineConfig
        pod = kubernetes_config.build_pod(
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
        )

        self.kube_dbnd.run_pod(pod, task_run=task_run)

    def delete_pod(self, pod_id):
        if self.kube_config.delete_worker_pods and not (
            self.kube_dbnd.engine_config.keep_failed_pods
            and self.kube_dbnd.get_pod_state(pod_id, self.namespace) == State.FAILED
        ):
            super(DbndKubernetesScheduler, self).delete_pod(pod_id=pod_id)


class DbndKubernetesExecutor(KubernetesExecutor):
    def __init__(self, kube_dbnd=None):
        # type: (DbndKubernetesExecutor, DbndKubernetesClient) -> None
        super(DbndKubernetesExecutor, self).__init__()
        self._manager = multiprocessing.Manager()

        self.kube_dbnd = kube_dbnd
        _update_airflow_kube_config(
            airflow_kube_config=self.kube_config, engine_config=kube_dbnd.engine_config
        )

    def start(self):
        logger.info("Starting Kubernetes executor..")

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
        # we don't clear kubernetes tasks from previous run
        # self.clear_not_launched_queued_tasks()

    # override - by default UpdateQuery not working failing with
    # sqlalchemy.exc.CompileError: Unconsumed column names: state
    # due to model override
    # + we don't want to change tasks statuses - maybe they are managed by other executors
    @provide_session
    def clear_not_launched_queued_tasks(self, *args, **kwargs):
        pass

    def end(self):
        self.kube_dbnd.end()
        super(DbndKubernetesExecutor, self).end()

    def _change_state(self, key, state, pod_id):
        if state == State.FAILED:
            try:
                self.log.info("---printing failed pod logs----:")
                self.kube_dbnd.stream_pod_logs_helper(
                    pod_id, self.kube_config.kube_namespace, self.log, from_now=False
                )
            except Exception as err:
                self.log.error(
                    "error on reading failed pod logs %s on pod %s", err, pod_id
                )

        super(DbndKubernetesExecutor, self)._change_state(key, state, pod_id)

        if state != State.RUNNING:
            self.kube_dbnd.running_pods.pop(pod_id, None)


class DbndKubernetesJobWatcher(KubernetesJobWatcher):
    def run(self):
        try:
            super(DbndKubernetesJobWatcher, self).run()
        except KeyboardInterrupt:
            # because we convert SIGTERM to SIGINT without this you get an ugly exception in the log when
            # the executor terminates the watcher
            pass


class DbndPodLauncher(PodLauncher):
    def __init__(self, *args, **kwargs):
        kube_dbnd = kwargs.pop("kube_dbnd")
        super(DbndPodLauncher, self).__init__(*args, **kwargs)

        self.kube_dbnd = kube_dbnd
        self.resource_request_above_max_capacity_checked = False
        self.kube_req_factory = DbndPodRequestFactory()

    def _task_status(self, event):
        # PATCH: info -> debug
        self.log.debug(
            "Event: %s had an event of type %s", event.metadata.name, event.status.phase
        )
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod):
        v1_pod = self.read_pod(pod)

        # PATCH:  validate deploy errors
        self.kube_dbnd.check_deploy_errors(v1_pod)

        state = self._task_status(v1_pod)
        return state == State.QUEUED
