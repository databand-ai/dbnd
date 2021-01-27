import logging

import pendulum
import pytest

from airflow.models import TaskInstance
from mock import Mock

from dbnd_airflow.tracking.dbnd_airflow_handler import DbndAirflowHandler


def airflow_log_factory(ti, try_number):
    return "{dag_id}/{task_id}/{execution_date}/{try_number}.log".format(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        execution_date=ti.execution_date,
        try_number=try_number,
    )


class TestDbndAirflowHandler(object):
    @pytest.fixture(scope="function")
    def dbnd_airflow_handler(self):
        return DbndAirflowHandler(
            logger=Mock(logging.getLoggerClass()),
            local_base="/logger_base",
            log_file_name_factory=airflow_log_factory,
        )

    @pytest.fixture(scope="function")
    def ti(self):
        task_instance = Mock(TaskInstance)
        task_instance.task_id = "task"
        task_instance.dag_id = "dag"
        task_instance.execution_date = pendulum.datetime(1970, 1, 1)
        task_instance.try_number = 1
        return task_instance

    @pytest.fixture
    def patched_context(self):
        context = Mock()
        context.tracking_store = Mock()
        return context

    def test_set_context(self, monkeypatch, patched_context, dbnd_airflow_handler, ti):
        monkeypatch.setattr(
            "dbnd_airflow.tracking.dbnd_airflow_handler.config.getboolean",
            lambda x, y: False,
        )
        monkeypatch.setattr(
            "dbnd_airflow.tracking.dbnd_airflow_handler.read_dbnd_log_preview",
            self.mock_read_lines,
        )

        monkeypatch.setattr(
            "dbnd_airflow.tracking.dbnd_airflow_handler.get_dbnd_project_config",
            self.mock_project_config,
        )

        # initial state
        assert dbnd_airflow_handler.log_file == ""
        assert dbnd_airflow_handler.dbnd_context is None
        assert dbnd_airflow_handler.task_run_attempt_uid is None
        assert dbnd_airflow_handler.dbnd_context_manage is None
        assert dbnd_airflow_handler.task_run_attempt_uid is None
        assert dbnd_airflow_handler.task_env_key is None

        # entering context
        dbnd_airflow_handler.set_context(ti)

        assert (
            dbnd_airflow_handler.log_file
            == "/logger_base/dag/task/1970-01-01T00:00:00+00:00/1.log"
        )
        assert dbnd_airflow_handler.dbnd_context is not None
        assert dbnd_airflow_handler.task_run_attempt_uid is not None

        assert dbnd_airflow_handler.dbnd_context_manage is not None
        assert (
            dbnd_airflow_handler.task_env_key
            == "DBND__TRACKING_ATTEMPT_UID:dag:task__execute"
        )

        # closing
        dbnd_airflow_handler.dbnd_context = patched_context
        dbnd_airflow_handler.close()
        assert patched_context.tracking_store.save_task_run_log.call_count == 1
        assert dbnd_airflow_handler.dbnd_context is None
        assert dbnd_airflow_handler.dbnd_context_manage is None
        assert dbnd_airflow_handler.task_env_key is None

    def test_set_context_with_conflict(self, monkeypatch, dbnd_airflow_handler, ti):
        monkeypatch.setattr(
            "dbnd_airflow.tracking.dbnd_airflow_handler.config.getboolean",
            lambda x, y: True,
        )

        dbnd_airflow_handler.set_context(ti)
        assert dbnd_airflow_handler.dbnd_context is None

    def mock_read_lines(self, *args, **kwargs):
        return ["line1", "line2"]

    def mock_project_config(self):
        proj_conf = Mock()
        proj_conf.is_tracking_mode.return_value = True
        return proj_conf
