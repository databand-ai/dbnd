from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import logging
import sys
import time
import warnings

from airflow.exceptions import (
    AirflowException,
    AirflowRescheduleException,
    AirflowSkipException,
    AirflowTaskTimeout,
)
from airflow.models.taskinstance  import XCOM_RETURN_KEY, Log, TaskInstance, Stats
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.net import get_hostname
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from six.moves.builtins import str

logger = logging.getLogger(__name__)


# DO NOT REFORMAT THIS FILE!
# WE WANT TO BE ABLE TO COMPARE IT TO ORIGINAL

class DbndAirflowTaskInstance(TaskInstance):
    @staticmethod
    @provide_session
    def generate_command(
        dag_id,
        task_id,
        execution_date,
        mark_success=False,
        ignore_all_deps=False,
        ignore_depends_on_past=False,
        ignore_task_deps=False,
        ignore_ti_state=False,
        local=False,
        pickle_id=None,
        file_path=None,
        raw=False,
        job_id=None,
        pool=None,
        cfg_path=None,
        session=None,
    ):
        """
        Generates the shell command required to execute this task instance.
        :return: shell command that can be used to run the task instance
        """
        cmd = TaskInstance.generate_command(
            dag_id,
            task_id,
            execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=file_path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path,
        )

        # we need to get into Task Instance Configuration
        # there can be DatabandExecutor configuration
        TI = TaskInstance
        ti = (
            session.query(TI.executor_config)
            .filter(
                TI.task_id == task_id,
                TI.dag_id == dag_id,
                TI.execution_date == execution_date,
            )
            .first()
        )
        if ti and ti.executor_config and "DatabandExecutor" in ti.executor_config:
            # we are running databand task
            cmd[1] = "run-task-airflow"
            driver_dump = ti.executor_config["DatabandExecutor"].get("dbnd_driver_dump")
            cmd.extend(["--dbnd-run", driver_dump])
        else:
            cmd[1:2] = ["airflow", "run"]

        from dbnd._core.current import try_get_databand_run
        dr = try_get_databand_run()
        if dr:
            task_run = dr.get_task_run_by_af_id(task_id)
            if task_run:
                return task_run.task_engine.dbnd_executable + cmd[1:]
        # we want to run in the same environment with databand
        return [sys.executable, "-m", "dbnd"] + cmd[1:]

    @provide_session
    def current_state(self, session=None):
        """
        DBNDPATCH: we replace logic that fetch state from DB
        without this patch scheduler job fetch new state -> it takes O(n) requests to do scheduler.tick()
        """
        return self.state

    # COPY PASTE FROM model.py
    # we need to be able to override op.execute() func
    # however, we copy the object, so we can't just monkey patch the original object
    # if we monkey patch - after the copy.copy the method will use the old object as a self
    # the only solution we have for now is to replace the way we call execute
    @provide_session
    def _run_raw_task(
        self, mark_success=False, test_mode=False, job_id=None, pool=None, session=None
    ):
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.__class__.__name__

        context = {}
        actual_start_date = timezone.utcnow()
        try:
            if not mark_success:
                context = self.get_template_context()

                task_copy = copy.copy(task)
                self.task = task_copy

                # def signal_handler(signum, frame):
                #     self.log.error("Received SIGTERM. Terminating subprocesses.")
                #     task_copy.on_kill()
                #     raise AirflowException("Task received SIGTERM signal")
                # signal.signal(signal.SIGTERM, signal_handler)

                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                start_time = time.time()

                self.render_templates()
                task_copy.pre_execute(context=context)

                from dbnd_airflow.dbnd_task_executor.dbnd_execute import dbnd_execute_airflow_operator
                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(task_copy.execution_timeout.total_seconds())):
                            result = dbnd_execute_airflow_operator(task_copy, context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = dbnd_execute_airflow_operator(task_copy, context=context)

                # If the task returns a result, push an XCom containing it
                if result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                # TODO remove deprecated behavior in Airflow 2.0
                try:
                    task_copy.post_execute(context=context, result=result)
                except TypeError as e:
                    if 'unexpected keyword argument' in str(e):
                        warnings.warn(
                            'BaseOperator.post_execute() now takes two '
                            'arguments, `context` and `result`, but "{}" only '
                            'expected one. This behavior is deprecated and '
                            'will be removed in a future version of '
                            'Airflow.'.format(self.task_id),
                            category=DeprecationWarning)
                        task_copy.post_execute(context=context)
                    else:
                        raise

                end_time = time.time()
                duration = end_time - start_time
                Stats.timing(
                    'dag.{dag_id}.{task_id}.duration'.format(
                        dag_id=task_copy.dag_id,
                        task_id=task_copy.task_id),
                    duration)

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException:
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
        except AirflowRescheduleException as reschedule_exception:
            self.refresh_from_db()
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode, context)
            return
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                return
            else:
                self.handle_failure(e, test_mode, context)
                raise
        except KeyboardInterrupt as e:
            logger.error("User Interrupt! Killing the task..")
            self.handle_failure(e, test_mode, context)
            # otherwise it can retry
            self.set_state(State.FAILED)
            raise
        except Exception as e:
            self.handle_failure(e, test_mode, context)
            raise

        # Success callback
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as e3:
            self.log.error("Failed when executing success callback")
            self.log.exception(e3)

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()
