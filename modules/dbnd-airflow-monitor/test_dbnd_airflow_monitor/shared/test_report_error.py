# © Copyright Databand.ai, an IBM Company 2022

import uuid

from unittest.mock import Mock

import pytest

from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)


class TestReportError:
    @pytest.fixture
    def integration_management_service(self):
        service = IntegrationManagementService("type", BaseServerConfig, {})
        service._api_client = Mock()
        return service

    def test_report_error(self, integration_management_service):
        integration_management_service.report_error(
            integration_uid=uuid.uuid4(),
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": "msg"}

    def test_dont_report_error(self, integration_management_service):
        integration_management_service.report_error(
            integration_uid=uuid.uuid4(),
            full_function_name="Syncer.sync_once",
            err_message=None,
        )
        assert not integration_management_service._api_client.api_request.call_count

    def test_clear_error(self, integration_management_service):
        integration_uid = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": "msg"}

        integration_management_service.report_error(
            integration_uid=integration_uid,
            full_function_name="Syncer.sync_once",
            err_message=None,
        )
        assert integration_management_service._api_client.api_request.call_count == 2

        second_report = self._get_call_kwargs(integration_management_service, 1)
        assert second_report["data"] == {"monitor_error_message": None}

    def test_clean_error_message_first(self, integration_management_service):
        integration_uid = uuid.uuid4()
        integration_management_service.clean_error_message(
            integration_uid=integration_uid
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": None}

        integration_management_service.report_error(
            integration_uid=integration_uid,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        second_report = self._get_call_kwargs(integration_management_service, 1)
        assert second_report["data"] == {"monitor_error_message": "msg"}

    def test_clean_error_message_after_existing_error(
        self, integration_management_service
    ):
        integration_uid = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": "msg"}

        integration_management_service.clean_error_message(
            integration_uid=integration_uid
        )

        second_report = self._get_call_kwargs(integration_management_service, 1)
        assert second_report["data"] == {"monitor_error_message": None}

    def test_override_error(self, integration_management_service):
        integration_uid_1 = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": "msg"}

        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer.sync_once",
            err_message="msg 2",
        )

        second_report = self._get_call_kwargs(integration_management_service, 1)
        assert second_report["data"] == {"monitor_error_message": "msg 2"}

    def test_aggregate_errors(self, integration_management_service):
        integration_uid_1 = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": "msg"}

        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer2.sync_once",
            err_message="msg 2",
        )

        second_report = self._get_call_kwargs(integration_management_service, 1)
        assert second_report["data"] == {
            "monitor_error_message": "msg 2\n\n---------\n\nmsg"
        }

    def test_separate_by_integration_uid(self, integration_management_service):
        integration_uid_1 = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 0)
        assert first_report["data"] == {"monitor_error_message": "msg"}

        integration_uid_2 = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid_2,
            full_function_name="Syncer2.sync_once",
            err_message="msg 2",
        )

        second_report = self._get_call_kwargs(integration_management_service, 1)
        assert second_report["data"] == {"monitor_error_message": "msg 2"}

    def test_simulate_two_integrations(self, integration_management_service):
        integration_uid_1 = uuid.uuid4()
        integration_uid_2 = uuid.uuid4()

        # first thing that happened is that we clean the error msgs exciting for each integration
        integration_management_service.clean_error_message(
            integration_uid=integration_uid_1
        )
        integration_management_service.clean_error_message(
            integration_uid=integration_uid_2
        )

        # First integration first error
        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer.sync_once",
            err_message="msg",
        )

        first_report = self._get_call_kwargs(integration_management_service, 2)
        assert first_report["data"] == {"monitor_error_message": "msg"}

        # Second integration no error
        integration_uid_2 = uuid.uuid4()
        integration_management_service.report_error(
            integration_uid=integration_uid_2,
            full_function_name="Syncer2.sync_once",
            err_message=None,
        )

        # no new data to update
        assert integration_management_service._api_client.api_request.call_count == 3

        # First integration second error
        integration_management_service.report_error(
            integration_uid=integration_uid_1,
            full_function_name="Syncer2.sync_once",
            err_message="msg 2",
        )

        second_report = self._get_call_kwargs(integration_management_service, 3)
        assert second_report["data"] == {
            "monitor_error_message": "msg 2\n\n---------\n\nmsg"
        }

    def _get_call_kwargs(self, integration_management_service, i):
        (
            _,
            kwargs,
        ) = integration_management_service._api_client.api_request.call_args_list[i]
        return kwargs
