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
import multiprocessing

from airflow.contrib.executors.kubernetes_executor import (
    AirflowKubernetesScheduler,
    KubeConfig,
    KubernetesExecutor,
    KubernetesJobWatcher,
)
from airflow.models import KubeWorkerIdentifier, TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State

from dbnd._core.constants import TaskRunState
from dbnd._core.current import try_get_databand_run
from dbnd._core.errors import DatabandError
from dbnd._core.task_build.task_registry import build_task_from_config
from dbnd_docker.kubernetes.kube_dbnd_client import DbndKubernetesClient
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


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
        self.kube_dbnd = kube_dbnd

        # PATCH manage watcher
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.current_resource_version = 0
        self.kube_watcher = self._make_kube_watcher_dbnd()

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

        self.kube_dbnd.run_pod(pod=pod, task_run=task_run)

    def delete_pod(self, pod_id):
        return self.kube_dbnd.delete_pod(pod_id, self.namespace)


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


class DbndKubernetesJobWatcher(KubernetesJobWatcher):
    def __init__(self, kube_dbnd, **kwargs):
        super(DbndKubernetesJobWatcher, self).__init__(**kwargs)
        self.kube_dbnd = kube_dbnd

    def run(self):
        try:
            super(DbndKubernetesJobWatcher, self).run()
        except KeyboardInterrupt:
            # because we convert SIGTERM to SIGINT without this you get an ugly exception in the log when
            # the executor terminates the watcher
            pass

    def _run(self, kube_client, resource_version, worker_uuid, kube_config):
        self.log.info(
            "Event: and now my watch begins starting at resource_version: %s",
            resource_version,
        )

        from kubernetes import watch

        watcher = watch.Watch()

        kwargs = {"label_selector": "airflow-worker={}".format(worker_uuid)}
        if resource_version:
            kwargs["resource_version"] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version = None
        for event in watcher.stream(
            kube_client.list_namespaced_pod, self.namespace, **kwargs
        ):
            task = event["object"]
            self.log.info(
                "Event: %s had an event of type %s", task.metadata.name, event["type"]
            )
            if event["type"] == "ERROR":
                return self.process_error(event)

            # DBND PATCH
            # we want to process
            self.dbnd_process(task)
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def dbnd_process(self, pod_data):
        pod_name = pod_data.metadata.name
        phase = pod_data.status.phase
        if phase == "Pending":
            self.log.info("Event: %s Pending", pod_name)
            pod_ctrl = self.kube_dbnd.get_pod_ctrl(name=pod_name)
            try:
                pod_ctrl.check_deploy_errors(pod_data)
            except Exception as ex:
                self.dbnd_set_task_pending_fail(pod_data, ex)
                phase = "Failed"
        elif phase == "Failed":
            self.dbnd_set_task_failed(pod_data)

        self.process_status(
            pod_data.metadata.name,
            phase,
            pod_data.metadata.labels,
            pod_data.metadata.resource_version,
        )

    def _get_task_run(self, pod_data):
        labels = pod_data.metadata.labels
        if "task_id" not in labels:
            return None
        task_id = labels["task_id"]

        dr = try_get_databand_run()
        if not dr:
            return None

        return dr.get_task_run_by_af_id(task_id)

    def dbnd_set_task_pending_fail(self, pod_data, ex):
        task_run = self._get_task_run(pod_data)
        from dbnd._core.task_run.task_run_error import TaskRunError

        task_run_error = TaskRunError.buid_from_ex(ex, task_run)
        task_run.set_task_run_state(TaskRunState.FAILED, error=task_run_error)

    def dbnd_set_task_failed(self, pod_data):
        metadata = pod_data.metadata
        logger.debug("getting failure info")
        # noinspection PyBroadException
        pod_ctrl = self.kube_dbnd.get_pod_ctrl(metadata.name, metadata.namespace)
        logs = []
        try:
            log_printer = lambda x: logs.append(x)
            pod_ctrl.stream_pod_logs(
                print_func=log_printer, tail_lines=40, follow=False
            )
        except Exception:
            logger.exception("failed to get log")

        logger.debug("Getting task run")
        task_run = self._get_task_run(pod_data)
        if not task_run:
            logger.info("no task run")
            return

        from dbnd._core.task_run.task_run_error import TaskRunError

        # work around to build an error object
        try:
            raise DatabandError(
                "Pod %s at %s has failed with: \n%s"
                % (metadata.name, metadata.namespace, "\n".join(logs)),
                show_exc_info=False,
                help_msg="Please see full pod log for more details",
            )
        except DatabandError as ex:
            error = TaskRunError.buid_from_ex(ex, task_run)

        task_state = self._get_airflow_task_instance_state(task_run=task_run)

        logger.info("task airflow state: %s ", task_state)
        if task_state == State.FAILED:
            # let just notify the error, so we can show it in summary it
            # we will not send it to databand tracking store
            task_run.set_task_run_state(TaskRunState.FAILED, track=False, error=error)
        else:
            task_run.set_task_run_state(TaskRunState.FAILED, track=True, error=error)
            task_run.tracker.save_task_run_log("\n".join(logs))

    @provide_session
    def _get_airflow_task_instance_state(self, task_run, session=None):
        TI = TaskInstance
        return (
            session.query(TI.state)
            .filter(
                TI.dag_id == task_run.task.ctrl.airflow_op.dag.dag_id,
                TI.task_id == task_run.task_af_id,
                TI.execution_date == task_run.run.execution_date,
            )
            .scalar()
        )
