# © Copyright Databand.ai, an IBM Company 2022

import pytest

from mock import patch

from dbnd.api.databand_client import DatabandClient


@pytest.fixture
def databand_client():
    return DatabandClient.build_databand_client()


@patch("dbnd.api.databand_client.DatabandClient.get_run_info")
def test_get_first_task_run_error(mock_get_run_info, databand_client):
    mock_get_run_info.return_value = {
        "task_runs": [
            {
                "latest_error": {
                    "msg": "Task was killed by the user",
                    "help_msg": "task with task_run_uid:bf8707be-0a78-11ec-9193-acde48001122 initiated kill_run",
                    "databand_error": True,
                    "show_exc_info": False,
                    "user_code_traceback": '\x1b[0;34mTraceback (most recent call last):\x1b[m\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\x1b[1;31mdbnd._core.errors.base.DatabandError\x1b[m: \x1b[0;32mTask was killed by the user\x1b[m\n\n',
                    "nested": "",
                    "traceback": 'Traceback (most recent call last):\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\ndbnd._core.errors.base.DatabandError: Task was killed by the user\n',
                },
                "latest_task_run_attempt": {
                    "timestamp": "2021-08-31T16:30:40.760066+00:00",
                    "start_date": "2021-08-31T16:30:39.792293+00:00",
                    "end_date": "2021-08-31T16:30:40.760066+00:00",
                    "latest_error": {
                        "msg": "Task was killed by the user",
                        "help_msg": "task with task_run_uid:bf8707be-0a78-11ec-9193-acde48001122 initiated kill_run",
                        "databand_error": True,
                        "show_exc_info": False,
                        "user_code_traceback": '\x1b[0;34mTraceback (most recent call last):\x1b[m\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\x1b[1;31mdbnd._core.errors.base.DatabandError\x1b[m: \x1b[0;32mTask was killed by the user\x1b[m\n\n',
                        "nested": "",
                        "traceback": 'Traceback (most recent call last):\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\ndbnd._core.errors.base.DatabandError: Task was killed by the user\n',
                    },
                },
            }
        ],
        "run_uid": "bf57cba2-0a78-11ec-b5ea-acde48001122",
    }
    actual = databand_client.get_first_task_run_error("mock-run-uid")
    expected = {
        "msg": "Task was killed by the user",
        "help_msg": "task with task_run_uid:bf8707be-0a78-11ec-9193-acde48001122 initiated kill_run",
        "databand_error": True,
        "show_exc_info": False,
        "user_code_traceback": '\x1b[0;34mTraceback (most recent call last):\x1b[m\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\x1b[1;31mdbnd._core.errors.base.DatabandError\x1b[m: \x1b[0;32mTask was killed by the user\x1b[m\n\n',
        "nested": "",
        "traceback": 'Traceback (most recent call last):\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\ndbnd._core.errors.base.DatabandError: Task was killed by the user\n',
    }
    assert actual == expected


@patch("dbnd.api.databand_client.DatabandClient.get_run_info")
def test_get_first_task_run_error_with_no_end_date(mock_get_run_info, databand_client):
    mock_get_run_info.return_value = {
        "task_runs": [
            {
                "latest_error": {
                    "msg": "Task was killed by the user",
                    "help_msg": "task with task_run_uid:bf8707be-0a78-11ec-9193-acde48001122 initiated kill_run",
                    "databand_error": True,
                    "show_exc_info": False,
                    "user_code_traceback": '\x1b[0;34mTraceback (most recent call last):\x1b[m\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\x1b[1;31mdbnd._core.errors.base.DatabandError\x1b[m: \x1b[0;32mTask was killed by the user\x1b[m\n\n',
                    "nested": "",
                    "traceback": 'Traceback (most recent call last):\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\ndbnd._core.errors.base.DatabandError: Task was killed by the user\n',
                },
                "latest_task_run_attempt": {
                    "timestamp": "2021-08-31T16:30:40.760066+00:00",
                    "start_date": "2021-08-31T16:30:39.792293+00:00",
                    "end_date": None,
                    "latest_error": {
                        "msg": "Task was killed by the user",
                        "help_msg": "task with task_run_uid:bf8707be-0a78-11ec-9193-acde48001122 initiated kill_run",
                        "databand_error": True,
                        "show_exc_info": False,
                        "user_code_traceback": '\x1b[0;34mTraceback (most recent call last):\x1b[m\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\x1b[1;31mdbnd._core.errors.base.DatabandError\x1b[m: \x1b[0;32mTask was killed by the user\x1b[m\n\n',
                        "nested": "",
                        "traceback": 'Traceback (most recent call last):\n  File "/Users/adam_elgressy/dbnd-alpha/databand/dbnd-core/modules/dbnd/src/dbnd/_core/task_run_executor/task_run_error.py", line 60, in build_from_message\n    msg, show_exc_info=False, help_msg=help_msg,\ndbnd._core.errors.base.DatabandError: Task was killed by the user\n',
                    },
                },
            }
        ],
        "run_uid": "bf57cba2-0a78-11ec-b5ea-acde48001122",
    }
    actual = databand_client.get_first_task_run_error("mock-run-uid")
    expected = None
    assert actual == expected
