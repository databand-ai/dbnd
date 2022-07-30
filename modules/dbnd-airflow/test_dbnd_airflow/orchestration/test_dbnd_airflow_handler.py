# © Copyright Databand.ai, an IBM Company 2022

import logging

import mock
import pendulum
import pytest

from airflow.models import TaskInstance
from mock import Mock

from dbnd_airflow.tracking.dbnd_airflow_handler import DbndAirflowHandler


class TestDbndAirflowHandler(object):
    @pytest.fixture(scope="function")
    def dbnd_airflow_handler(self):
        dbnd_airflow_handler = DbndAirflowHandler(logger=Mock(logging.getLoggerClass()))
        return dbnd_airflow_handler

    @pytest.fixture(scope="function")
    def ti(self):
        task_instance = Mock(TaskInstance)
        task_instance.task_id = "task"
        task_instance.dag_id = "dag"
        task_instance.execution_date = pendulum.datetime(1970, 1, 1)
        task_instance.try_number = 1
        task_instance.raw = True
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
            "dbnd_airflow.tracking.dbnd_airflow_handler.get_dbnd_project_config",
            self.mock_project_config,
        )

        # initial state
        assert dbnd_airflow_handler.dbnd_context is None
        assert dbnd_airflow_handler.in_memory_log_manager is None
        assert dbnd_airflow_handler.task_run_attempt_uid is None
        assert dbnd_airflow_handler.dbnd_context_manage is None
        assert dbnd_airflow_handler.task_run_attempt_uid is None
        assert dbnd_airflow_handler.task_env_key is None

        # entering context
        dbnd_airflow_handler.set_context(ti)
        assert dbnd_airflow_handler.dbnd_context is not None
        assert dbnd_airflow_handler.task_run_attempt_uid is not None
        assert dbnd_airflow_handler.in_memory_log_manager is not None
        assert dbnd_airflow_handler.dbnd_context_manage is not None
        assert (
            dbnd_airflow_handler.task_env_key == "DBND__TRACKING_ATTEMPT_UID:dag:task"
        )

        # patching created in memory log manager
        mock.patch.object(
            dbnd_airflow_handler.in_memory_log_manager,
            "get_log_body",
            new_callable=self.mock_read_lines,
        )
        # closing
        dbnd_airflow_handler.dbnd_context = patched_context
        dbnd_airflow_handler.close()
        assert patched_context.tracking_store.save_task_run_log.call_count == 1
        assert dbnd_airflow_handler.dbnd_context is None
        assert dbnd_airflow_handler.in_memory_log_manager is None
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
