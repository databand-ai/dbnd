import logging
import os

from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.log.file_task_handler import FileTaskHandler
from more_itertools import first_true

import dbnd

from dbnd import config, get_dbnd_project_config
from dbnd._core.constants import AD_HOC_DAG_PREFIX
from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.task_run.log_preview import read_dbnd_log_preview
from dbnd._core.tracking.airflow_dag_inplace_tracking import calc_task_key_from_af_ti
from dbnd._core.utils.basics import environ_utils
from dbnd._core.utils.uid_utils import get_task_run_attempt_uid_from_af_ti
from dbnd_airflow.tracking.execute_tracking import is_dag_eligable_for_tracking
from dbnd_airflow.tracking.fakes import FakeTaskRun


AIRFLOW_FILE_TASK_HANDLER = FileTaskHandler.__name__

AIRFLOW_TASK_LOGGER = "airflow.task"


class DbndAirflowHandler(logging.Handler):
    """
    This is a logger handler that is used as an Entry Point to airflow run.
    It's injected to the Logger(name="airflow.task"), and used by entering the context on the beginning of the task
    instance run, and getting close when the task instance is done.
    """

    def __init__(self, logger, local_base, log_file_name_factory):
        logging.Handler.__init__(self)

        self.dbnd_context = None
        self.dbnd_context_manage = None

        self.task_run_attempt_uid = None
        self.task_env_key = None

        self.airflow_logger = logger
        self.airflow_base_log_dir = local_base
        self.log_file_name_factory = log_file_name_factory

        self.log_file = ""

    def set_context(self, ti):
        """
        Airflow's log handler use this method to setup the context when running a TaskInstance(=ti).
        We use this method to setup the dbnd context and communicate information to
        the `<airflow_operator>_execute` task, that we create in `execute_tracking.py`.
        """
        # we setting up only when we are not in our own orchestration dag
        if ti.dag_id.startswith(AD_HOC_DAG_PREFIX):
            return

        if not is_dag_eligable_for_tracking(ti.dag_id):
            return

        if config.getboolean("mlflow_tracking", "databand_tracking"):
            self.airflow_logger.warning(
                "dbnd can't track mlflow and airflow together please disable dbnd config "
                "`databand_tracking` in section `mlflow_tracking`"
            )
            return

        # we are not tracking SubDagOperator
        if ti.operator is None or ti.operator == SubDagOperator.__name__:
            return

        # Airflow is running with two process `run` and `--raw run`.
        # But we want the handler to run only once (Idempotency)
        # So we are using an environment variable to sync those two process
        task_key = calc_task_key_from_af_ti(ti)
        if os.environ.get(task_key, False):
            # This key is already set which means we are in `--raw run`
            return
        else:
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
            set_dbnd_config_from_airflow_connections()

        self.task_run_attempt_uid = get_task_run_attempt_uid_from_af_ti(ti)

        # airflow calculation for the relevant log_file
        log_relative_path = self.log_file_name_factory(ti, ti.try_number)
        self.log_file = os.path.join(self.airflow_base_log_dir, log_relative_path)

        # make sure we are not polluting the airflow logs
        get_dbnd_project_config().quiet_mode = True

        # tracking msg
        self.airflow_logger.info(
            "Databand Tracking Started {version}".format(version=dbnd.__version__)
        )

        # context with disabled logs
        self.dbnd_context_manage = new_dbnd_context(conf={"log": {"disabled": True}})
        self.dbnd_context = self.dbnd_context_manage.__enter__()

    def close(self):
        if self.dbnd_context:
            try:
                fake_task_run = FakeTaskRun(
                    task_run_attempt_uid=self.task_run_attempt_uid
                )

                log_body = read_dbnd_log_preview(self.log_file)
                self.dbnd_context.tracking_store.save_task_run_log(
                    task_run=fake_task_run,
                    log_body=log_body,
                    local_log_path=self.log_file,
                )

            except Exception:
                self.airflow_logger.exception("Exception occurred when saving task log")

        self.dbnd_context = None

        try:
            if self.dbnd_context_manage:
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
        """
        This handler is not really writing records, so ignoring.
        """


def set_dbnd_handler():
    """
    Build and inject the dbnd handler to airflow's logger.
    """
    airflow_logger = logging.getLogger(AIRFLOW_TASK_LOGGER)
    base_file_handler = first_true(
        airflow_logger.handlers,
        pred=lambda handler: handler.__class__.__name__ == AIRFLOW_FILE_TASK_HANDLER,
        default=None,
    )

    if base_file_handler:
        dbnd_handler = create_dbnd_handler(airflow_logger, base_file_handler)
        airflow_logger.addHandler(dbnd_handler)


def create_dbnd_handler(airflow_logger, airflow_file_handler):
    """
    Factory for creating dbnd handler with airflow's logger and airflow's file handler (<-log_handler)
    """
    return DbndAirflowHandler(
        logger=airflow_logger,
        local_base=airflow_file_handler.local_base,
        log_file_name_factory=airflow_file_handler._render_filename,
    )
