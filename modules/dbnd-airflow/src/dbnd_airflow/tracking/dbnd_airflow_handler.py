# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from typing import ContextManager, Optional

from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.log.file_task_handler import FileTaskHandler
from more_itertools import first_true

from dbnd import config, dbnd_bootstrap, dbnd_tracking_stop
from dbnd._core.constants import AD_HOC_DAG_PREFIX
from dbnd._core.context.databand_context import DatabandContext, new_dbnd_context
from dbnd._core.log import dbnd_log_debug, dbnd_log_exception
from dbnd._core.log.buffered_log_manager import BufferedLogManager
from dbnd._core.tracking.airflow_dag_inplace_tracking import calc_task_key_from_af_ti
from dbnd._core.tracking.script_tracking_manager import is_dbnd_tracking_active
from dbnd._core.utils.basics import environ_utils
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    get_dbnd_config_dict_from_airflow_connections,
    get_sync_status_and_tracking_dag_ids_from_dbnd_conf,
)
from dbnd_airflow.tracking.execute_tracking import is_dag_eligable_for_tracking
from dbnd_airflow.tracking.fakes import FakeTaskRun
from dbnd_airflow.utils import get_task_run_attempt_uid_from_af_ti


AIRFLOW_FILE_TASK_HANDLER = FileTaskHandler.__name__

AIRFLOW_TASK_LOGGER = "airflow.task"


class DbndAirflowLogHandler(logging.Handler):
    """
    This is a logger handler that is used as an Entry Point to airflow run.
    It's injected to the Logger(name="airflow.task"), and used by entering the context on the beginning of the task
    instance run, and getting close when the task instance is done.
    """

    def __init__(self, logger):
        logging.Handler.__init__(self)

        self.dbnd_context: Optional[DatabandContext] = None
        self.dbnd_context_manage: Optional[ContextManager[DatabandContext]] = None

        self.task_run_attempt_uid = None
        self.task_env_key = None

        self.airflow_logger = logger

        self.in_memory_log_manager: Optional[BufferedLogManager] = None

    def _set_from_airflow_handler(self):
        airflow_logger = self.airflow_logger
        inheriting_handler = first_true(
            airflow_logger.handlers,
            pred=lambda handler: handler.__class__.__name__
            == AIRFLOW_FILE_TASK_HANDLER,
            default=airflow_logger.handlers[0] if airflow_logger.handlers else None,
        )
        if inheriting_handler:
            handler_formatter = inheriting_handler.formatter
            self.setFormatter(handler_formatter)
            for handler_filter in inheriting_handler.filters:
                self.addFilter(handler_filter)

    def set_context(self, ti):
        try:
            self._set_context(ti)
        except Exception:
            dbnd_log_exception("Failed to initialize DbndAirflowHandler")

    def _set_context(self, ti):
        """
        Airflow's log handler use this method to setup the context when running a TaskInstance(=ti).
        We use this method to setup the dbnd context and communicate information to
        the `<airflow_operator>_execute` task, that we create in `execute_tracking.py`.
        """
        # we setting up only when we are not in our own orchestration dag
        if ti.dag_id.startswith(AD_HOC_DAG_PREFIX):
            return

        dbnd_config_from_connection = get_dbnd_config_dict_from_airflow_connections()
        (
            sync_enabled,
            tracking_list,
        ) = get_sync_status_and_tracking_dag_ids_from_dbnd_conf(
            dbnd_config_from_connection
        )
        if not sync_enabled:
            return

        if not is_dag_eligable_for_tracking(ti.dag_id, tracking_list=tracking_list):
            return

        # we are not tracking SubDagOperator
        if ti.operator is None or ti.operator == SubDagOperator.__name__:
            return

        # make sure we are not polluting the airflow logs
        dbnd_bootstrap()

        if config.getboolean("mlflow_tracking", "databand_tracking"):
            self.airflow_logger.warning(
                "dbnd can't track mlflow and airflow together please disable dbnd config "
                "`databand_tracking` in section `mlflow_tracking`"
            )
            return

        # Airflow is running with two process `--local` and `--raw`.
        # But we want the handler to run only once (Idempotency)
        # So we are using an environment variable to sync those two process
        task_key = calc_task_key_from_af_ti(ti)

        if ti.raw:

            from dbnd._core.settings.tracking_log_config import TrackingLoggingConfig

            logger_config = TrackingLoggingConfig()

            self.in_memory_log_manager = BufferedLogManager(
                max_head_bytes=logger_config.preview_head_bytes,
                max_tail_bytes=logger_config.preview_tail_bytes,
            )
            dbnd_log_debug(
                f"Initiated In Memory Log Manager with task {task_key}: "
                f"head={logger_config.preview_head_bytes} "
                f"tail={logger_config.preview_tail_bytes}, "
                f"context={ self.dbnd_context is not None }. "
            )

        if os.environ.get(task_key):
            # This key is already set which means we are in `--raw run`
            return

        # We are in the outer `run`
        self.task_env_key = task_key
        # marking the environment with the current key for the
        environ_utils.set_on(task_key)

        from dbnd_airflow.tracking.dbnd_airflow_conf import (
            set_dbnd_config_from_airflow_connections,
        )

        # When we are in `--raw run`, in tracking, it runs the main airflow process
        # for every task, which made some of the features to run twice,
        # once when the `worker` process ran, and once when the `main` one ran,
        # which made some of the features to run with different configurations.
        # it still runs twice, but know with the same configurations.
        set_dbnd_config_from_airflow_connections(
            dbnd_config_from_connection=dbnd_config_from_connection
        )

        self.task_run_attempt_uid = get_task_run_attempt_uid_from_af_ti(ti)

        # context with disabled logs
        self.dbnd_context_manage: ContextManager[DatabandContext] = new_dbnd_context()
        self.dbnd_context = self.dbnd_context_manage.__enter__()

    def close(self):
        if self.dbnd_context and self.in_memory_log_manager:
            try:
                fake_task_run = FakeTaskRun(
                    task_run_attempt_uid=self.task_run_attempt_uid
                )
                in_memory_log_body = self.in_memory_log_manager.get_log_body()
                self.dbnd_context.tracking_store.save_task_run_log(
                    task_run=fake_task_run, log_body=in_memory_log_body
                )
                dbnd_log_debug(
                    "Saved Airflow log to Databand service (size=%s)"
                    % len(in_memory_log_body or "")
                )
            except Exception:
                self.airflow_logger.exception("Exception occurred when saving task log")

        self.dbnd_context = None
        self.in_memory_log_manager = None

        try:
            if self.dbnd_context_manage:
                if is_dbnd_tracking_active():
                    # Stops and clears the script tracking if exists and the user forgot to stop it.
                    self.airflow_logger.warning(
                        "A Databand Context from tracking is still active, did you forget to"
                        " use 'dbnd_tracking_stop'?\nWe encourage you to use 'dbnd_tracking' "
                        "context instead of 'dbnd_tracking_start' and 'dbnd_tracking_stop'"
                    )
                    dbnd_tracking_stop()

                self.dbnd_context_manage.__exit__(None, None, None)
        except Exception:
            self.airflow_logger.exception(
                "Exception occurred when trying to exit dbnd_context_manager"
            )
        finally:
            self.dbnd_context_manage = None

        try:
            if self.task_env_key and self.task_env_key in os.environ:
                del os.environ[self.task_env_key]
        except Exception:
            self.airflow_logger.exception(
                "Exception occurred when trying to remove task_env_key from env"
            )
        finally:
            self.task_env_key = None

    def emit(self, record):
        try:
            if self.in_memory_log_manager:
                msg = self.format(record)
                self.in_memory_log_manager.add_log_msg(msg)
        except RecursionError:  # See issue 36272
            # handle errors in log processing
            raise
        except Exception:
            self.handleError(record)


def add_dbnd_log_handler():
    """
    Build and inject the dbnd handler to airflow's logger.
    """
    try:
        airflow_logger = logging.getLogger(AIRFLOW_TASK_LOGGER)
        dbnd_handler = DbndAirflowLogHandler(logger=airflow_logger)
        airflow_logger.addHandler(dbnd_handler)
    except Exception:
        dbnd_log_exception("Failed to add dbnd log handler to airflow logger")
