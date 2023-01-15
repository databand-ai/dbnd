# © Copyright Databand.ai, an IBM Company 2022

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
#
# This file has been modified by databand.ai to support advanced kube configuration.

import json
import logging
import os
import typing
import uuid

from queue import Empty

from airflow.utils.db import provide_session
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

from dbnd._core.current import try_get_databand_run
from dbnd._core.log.logging_utils import PrefixLoggerAdapter
from dbnd_docker.kubernetes.kube_dbnd_client import DbndKubernetesClient
from dbnd_run.airflow.compat import AIRFLOW_VERSION_1, AIRFLOW_VERSION_2
from dbnd_run.airflow.compat.kubernetes_executor import KubernetesExecutor
from dbnd_run.airflow.executors.kubernetes_executor.kubernetes_scheduler import (
    DbndKubernetesScheduler,
    PodResult,
)
from dbnd_run.airflow.executors.kubernetes_executor.utils import (
    _update_airflow_kube_config,
)


if AIRFLOW_VERSION_1:
    from airflow.models import KubeWorkerIdentifier


if typing.TYPE_CHECKING:
    pass

MAX_POD_ID_LEN = 253


class DbndKubernetesExecutor(KubernetesExecutor):
    """
    Use custom dbdn implementation for scheduler and watcher
    Better handling of errors at pod submission
    Enables multiinstance run of KubernetesExecutor
    """

    def __init__(self, kube_dbnd=None):
        # type: (DbndKubernetesExecutor, DbndKubernetesClient) -> None
        from os import environ

        # This env variable is required for airflow's kubernetes configuration validation
        environ["AIRFLOW__KUBERNETES__DAGS_IN_IMAGE"] = "True"
        super(DbndKubernetesExecutor, self).__init__()

        self.kube_dbnd = kube_dbnd
        _update_airflow_kube_config(
            airflow_kube_config=self.kube_config, engine_config=kube_dbnd.engine_config
        )

        self._log = PrefixLoggerAdapter("k8s-executor", self.log)

    def start(self):
        self.log.info("Starting Kubernetes executor... PID: %s", os.getpid())

        dbnd_run = try_get_databand_run()
        if dbnd_run:
            if AIRFLOW_VERSION_2:
                self.worker_uuid = str(dbnd_run.run_uid)
            else:
                self.worker_uuid = (
                    KubeWorkerIdentifier.get_or_create_current_kube_worker_uuid()
                )
        else:
            self.worker_uuid = str(uuid.uuid4())

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

        if AIRFLOW_VERSION_1:
            self._inject_secrets()
        self.clear_not_launched_queued_tasks()
        self._flush_result_queue()

    # override - by default UpdateQuery not working failing with
    # sqlalchemy.exc.CompileError: Unconsumed column names: state
    # due to model override
    # + we don't want to change tasks statuses - maybe they are managed by other executors
    @provide_session
    def clear_not_launched_queued_tasks(self, session=None, *args, **kwargs):
        # we don't clear kubernetes tasks from previous run
        pass

    def sync(self):
        """Synchronize task state."""
        # DBND-AIRFLOW: copy paste of the initial function with some patches

        if self.running:
            self.log.debug("self.running: %s", self.running)
        if self.queued_tasks:
            self.log.debug("self.queued: %s", self.queued_tasks)
        self.kube_scheduler.sync()

        last_resource_version = None
        while True:
            try:
                results = self.result_queue.get_nowait()
                try:
                    result = PodResult.from_result(results)
                    last_resource_version = result.resource_version
                    self.log.info("Changing state of %s to %s", results, result.state)
                    try:
                        self._change_state(
                            result.key, result.state, result.pod_id, result.namespace
                        )
                    except Exception as e:
                        self.log.exception(
                            "Exception: %s when attempting "
                            + "to change state of %s to %s, re-queueing.",
                            e,
                            results,
                            result.state,
                        )
                        self.result_queue.put(results)
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

        # DBND-AIRFLOW: COMMENTED OUT, we have multiple executors, we can not use singleton for all airflow processes!
        # FIXED at Airflow 2.0
        # KubeResourceVersion.checkpoint_resource_version(last_resource_version)
        if last_resource_version is not None:
            self.kube_scheduler.current_resource_version = last_resource_version

        for _ in range(self.kube_config.worker_pods_creation_batch_size):
            try:
                task = self.task_queue.get_nowait()
                try:
                    self.kube_scheduler.run_next(task)
                except ApiException as e:
                    self.log.warning(
                        "ApiException when attempting to run task, re-queueing. "
                        "Message: %s" % json.loads(e.body)["message"]
                    )
                    # DBND-AIRFLOW: if we have some "mis configuration" - nothing will probably run, better abort.
                    if e.status in {403, 404}:
                        raise
                    self.task_queue.put(task)
                except HTTPError as e:
                    self.log.warning(
                        "HTTPError when attempting to run task, re-queueing. "
                        "Exception: %s",
                        str(e),
                    )
                    self.task_queue.put(task)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break

    def clear_zombie_task_instance(self, zombie_task_instance):
        if zombie_task_instance.key not in self.running:
            self.log.info(
                "Skipping zombie %s as not found at executor", zombie_task_instance
            )
            return

        zombie_pod_state = self.kube_scheduler.handle_zombie_task_instance(
            zombie_task_instance=zombie_task_instance
        )
        if not zombie_pod_state:
            return

        self._change_state(
            zombie_pod_state.scheduler_key, None, zombie_pod_state.pod_name
        )

    def _change_state(self, key, state, pod_id, namespace=None):
        if namespace is None:
            namespace = self.kube_scheduler.namespace

        return super(DbndKubernetesExecutor, self)._change_state(
            key, state, pod_id, namespace
        )
