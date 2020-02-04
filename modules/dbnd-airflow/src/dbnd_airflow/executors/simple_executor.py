# -*- coding: utf-8 -*-
#
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
import subprocess

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.dag_processing import SimpleTaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State

from dbnd._core import current
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.errors import DatabandError, show_error_once


logger = logging.getLogger(__name__)


class InProcessExecutor(BaseExecutor):
    """
    This executor will only run one task instance at a time, can be used
    for debugging. It is also the only executor that can be used with sqlite
    since sqlite doesn't support multiple connections.

    Does'nt start new process!
    """

    def __init__(self, dag_bag=None):
        super(InProcessExecutor, self).__init__()
        self.tasks_to_run = []
        self.fail_fast = config.getboolean("run", "fail_fast")
        self.dag_bag = dag_bag

    def execute_async(self, key, command, queue=None, executor_config=None):
        self.tasks_to_run.append((key, command, queue))

    def sync(self):
        task_failed = False
        for key, ti, pool in self.tasks_to_run:
            if self.fail_fast and task_failed:
                logger.info("Setting %s to %s", key, State.UPSTREAM_FAILED)
                ti.set_state(State.UPSTREAM_FAILED)
                self.change_state(key, State.UPSTREAM_FAILED)
                continue

            if current.is_killed():
                logger.info(
                    "Databand Context is killed! Stopping %s to %s", key, State.FAILED
                )
                ti.set_state(State.FAILED)
                self.change_state(key, State.FAILED)
                continue

            self.log.debug("Executing task: %s", ti)

            try:
                self._run_task_instance(ti, mark_success=False, pool=pool)
                self.change_state(key, State.SUCCESS)
            except subprocess.CalledProcessError as e:
                task_failed = True
                self.change_state(key, State.FAILED)
                self.log.error("Failed to execute task: %s.", str(e))
            except DatabandError as e:
                task_failed = True
                self.change_state(key, State.FAILED)
                self.log.error("Failed to execute task: %s.", str(e))
            except KeyboardInterrupt as e:
                task_failed = True
                fail_fast = True
                self.change_state(key, State.FAILED)
                self.log.exception("Interrupted to execute task: %s.", str(e))
            except Exception as e:
                task_failed = True
                self.change_state(key, State.FAILED)
                show_error_once.log_error(
                    self.log, e, "Failed to execute task %s: %s.", ti.task_id, str(e)
                )

        self.tasks_to_run = []

    def queue_command(self, simple_task_instance, command, priority=1, queue=None):
        key = simple_task_instance.key
        if key not in self.queued_tasks and key not in self.running:
            self.log.info("Adding to queue: %s", command)
            # we replace command with just task_instance
            command = simple_task_instance
            self.queued_tasks[key] = (command, priority, queue, simple_task_instance)
        else:
            self.log.info("could not queue task %s", key)

    def queue_task_instance(
        self,
        task_instance,
        mark_success=False,
        pickle_id=None,
        ignore_all_deps=False,
        ignore_depends_on_past=False,
        ignore_task_deps=False,
        ignore_ti_state=False,
        pool=None,
        cfg_path=None,
    ):

        self.queue_command(
            task_instance,
            None,
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue,
        )

    def end(self):
        # we are not async executor
        # so we can just return from the execution here
        for key, ti, pool in self.tasks_to_run:
            logger.info("Setting %s to %s", key, State.UPSTREAM_FAILED)
            ti.set_state(State.UPSTREAM_FAILED)
            self.change_state(key, State.UPSTREAM_FAILED)
        return
        # self.heartbeat()

    # overriding default implementation with better logging messages
    def change_state(self, key, state):
        logger.debug("popping: {}".format(key))
        self.running.pop(key)
        self.event_buffer[key] = state

    @provide_session
    def _run_task_instance(self, ti, mark_success, pool, session=None):
        # set proper state and try number to keep logger in sync
        if isinstance(ti, SimpleTaskInstance):
            from airflow.models import TaskInstance

            dag = self.dag_bag.get_dag(ti.dag_id)
            task = dag.get_task(ti.task_id)
            ti = TaskInstance(task, ti.execution_date)
        ti.state = State.RUNNING
        ti._try_number += 1
        # let save state
        session.merge(ti)
        session.commit()
        # backward compatible with airflow loggers
        from airflow.utils.log import logging_mixin

        logging_mixin.set_context(logging.root, ti)

        try:
            ti._run_raw_task(mark_success=mark_success, job_id=ti.job_id, pool=pool)
        finally:
            for handler in logging.root.handlers:
                if handler.name == "task":
                    handler.close()
