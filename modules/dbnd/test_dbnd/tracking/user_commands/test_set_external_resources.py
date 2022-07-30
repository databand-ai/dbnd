# © Copyright Databand.ai, an IBM Company 2022

import pytest

from more_itertools import last

from dbnd import task
from dbnd._core.current import try_get_current_task_run
from dbnd._core.tracking.commands import set_external_resource_urls
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_save_external_links


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestSetExternalResourceURLS(object):
    def test_set_external_resource_urls(self, mock_channel_tracker):
        @task()
        def task_with_set_external_resource_urls():
            set_external_resource_urls(
                {
                    "my_resource": "http://some_resource_name.com/path/to/resource/123456789"
                }
            )
            task_run = try_get_current_task_run()
            return task_run.task_run_attempt_uid

        task_run_attempt_uid = task_with_set_external_resource_urls()
        save_external_links_call = last(get_save_external_links(mock_channel_tracker))
        assert save_external_links_call["external_links_dict"] == {
            "my_resource": "http://some_resource_name.com/path/to/resource/123456789"
        }
        assert save_external_links_call["task_run_attempt_uid"] == str(
            task_run_attempt_uid
        )

    def test_set_external_resource_urls_without_links_values(
        self, mock_channel_tracker
    ):
        @task()
        def task_with_set_external_resource_urls():
            set_external_resource_urls({"resource": None})

        task_with_set_external_resource_urls()
        call = next(get_save_external_links(mock_channel_tracker), None)
        assert call is None

    def test_set_external_resource_urls_without_links(self, mock_channel_tracker):
        @task()
        def task_with_set_external_resource_urls():
            set_external_resource_urls({})

        task_with_set_external_resource_urls()
        call = next(get_save_external_links(mock_channel_tracker), None)
        assert call is None

    def test_set_external_resource_urls_without_running_task(
        self, mock_channel_tracker
    ):

        set_external_resource_urls(
            {"my_resource": "http://some_resource_name.com/path/to/resource/123456789"}
        )

        call = next(get_save_external_links(mock_channel_tracker), None)
        assert call is None
