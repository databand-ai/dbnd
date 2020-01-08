import contextlib
import os
import time

import pytest
import six

from mock import mock_open, patch

from dbnd._core.configuration.scheduler_file_config_loader import (
    InvalidConfigException,
    SchedulerFileConfigLoader,
)
from dbnd._core.utils.timezone import utcnow
from dbnd.api import scheduler_api_client


class TestSchedulerFileConfigLoader(object):
    @pytest.fixture
    def existing_jobs(self, monkeypatch):
        existing_jobs = []

        monkeypatch.setattr(
            scheduler_api_client,
            "get_scheduled_jobs",
            lambda *args, **kwargs: existing_jobs,
        )
        monkeypatch.setattr(os.path, "getmtime", lambda _: time.time())
        return existing_jobs

    @contextlib.contextmanager
    def build_delta(self, file):
        if six.PY2:
            open_name = "__builtin__.open"
        else:
            open_name = "builtins.open"

        with patch(open_name, mock_open(read_data=file)):
            loader = SchedulerFileConfigLoader(
                config_file="/somewhere/over/the/rainbow"
            )
            yield loader.build_delta()

    def test_simple_file(self, existing_jobs):
        file = """
        - name: dbnd_sanity_check
          cmd: dbnd run dbnd_sanity_check --task-version now
          schedule_interval: "* * * * *"
          retries: 2
          active: true
          
        - name: predict_wine_quality daily
          cmd: dbnd run predict_wine_quality --task-target-date {{ ds }}
          schedule_interval: "@daily"
          active: false
        """
        with self.build_delta(file) as delta_result:
            assert len(delta_result.to_create) == 2
            assert delta_result.to_create[0]["name"] == "dbnd_sanity_check"
            assert (
                delta_result.to_create[0]["cmd"]
                == "dbnd run dbnd_sanity_check --task-version now"
            )
            assert delta_result.to_create[0]["schedule_interval"] == "* * * * *"
            assert delta_result.to_create[0]["retries"] == 2
            assert delta_result.to_create[0]["active"] == True

            assert delta_result.to_create[1]["name"] == "predict_wine_quality daily"

            assert len(delta_result.to_disable) == 0
            assert len(delta_result.to_enable) == 0
            assert len(delta_result.to_update) == 0

    def test_broken_yaml(self, existing_jobs):
        with pytest.raises(InvalidConfigException):
            with self.build_delta("just a string"):
                pass

        with pytest.raises(InvalidConfigException):
            with self.build_delta("@@@"):  # special char
                pass

        # empty file is treated as an empty list of config entries
        with self.build_delta("") as delta_result:
            assert len(delta_result.to_update) == 0
            assert len(delta_result.to_enable) == 0
            assert len(delta_result.to_disable) == 0
            assert len(delta_result.to_create) == 0

    def test_validation_errors(self, existing_jobs):
        file = """
        - name: dbnd_sanity_check
          schedule_interval: "@ * * * *"
          retries: five
          active: BANANA
        
        - name: dbnd_sanity_check
        
        - retries: 1
        """

        with self.build_delta(file) as delta_results:
            first = delta_results.to_create[0]["validation_errors"]
            assert "active: Not a valid boolean" in first
            assert "cmd: Missing data for required field" in first
            assert "retries: Not a valid integer" in first
            assert "Invalid schedule_interval: [@ * * * *] is not acceptable" in first
            assert (
                "1 other entry exist in the configuration file with the same name"
                in first
            )

            second = delta_results.to_create[1]["validation_errors"]
            assert "schedule_interval: Missing data for required field" in second
            assert "cmd: Missing data for required field" in second
            assert (
                "1 other entry exist in the configuration file with the same name"
                in second
            )

            # nameless entry is not synced, only a log error is printed about it
            assert len(delta_results.to_create) == 2

    def test_update(self, existing_jobs):
        existing_jobs.extend(
            [
                {
                    "DbndScheduledJob": {
                        "name": "to be updated",
                        "cmd": "before",
                        "schedule_interval": "before",
                        "validation_errors": "before",
                    }
                },
                {"DbndScheduledJob": {"name": "to be disabled"}},
                {
                    "DbndScheduledJob": {
                        "name": "to be enabled",
                        "deleted_from_file": True,
                    }
                },
            ]
        )

        file = """
        - name: to be updated
          cmd: after
          schedule_interval: "* * * * *"
        
        - name: to be enabled
        """

        with self.build_delta(file) as delta_result:
            assert len(delta_result.to_update) == 1
            assert delta_result.to_update[0]["name"] == "to be updated"
            assert delta_result.to_update[0]["cmd"] == "after"
            assert delta_result.to_update[0]["schedule_interval"] == "* * * * *"

            assert len(delta_result.to_disable) == 1
            assert delta_result.to_disable[0]["name"] == "to be disabled"
            assert delta_result.to_disable[0]["deleted_from_file"]

            assert len(delta_result.to_enable) == 1
            assert delta_result.to_enable[0]["name"] == "to be enabled"
            assert not delta_result.to_enable[0]["deleted_from_file"]
