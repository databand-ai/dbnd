# © Copyright Databand.ai, an IBM Company 2022

import os

from random import choice
from unittest.mock import patch

import pytest

from dbnd_monitor.error_handling.error_handler import capture_component_exception


@pytest.fixture(scope="function")
def setup():
    path = "dbnd_monitor.error_handler"
    with (
        patch(f"{path}.capture_component_exception") as cm_default,
        patch(
            f"{path}.capture_component_exception_as_component_error"
        ) as cm_component_error,
    ):
        return cm_default, cm_component_error


def test_capture_component_exception(setup, generic_runtime_syncer):
    cm_default, cm_coponent_error = setup
    with capture_component_exception(generic_runtime_syncer, "test"):
        cm_default.assert_called_once_with(generic_runtime_syncer, "test")
        cm_coponent_error.assert_not_called()


@pytest.fixture
def update_env():
    env = {
        "DBND__MONITOR__COMPONENT_ERROR_SUPPORT": choice(["yes", "y", "1", "true", "t"])
    }
    with patch.dict(os.environ, env):
        yield


def test_capture_component_exception_custom(update_env, setup, generic_runtime_syncer):
    cm_default, cm_component_error = setup
    with capture_component_exception(generic_runtime_syncer, "test"):
        cm_default.assert_not_called()
        cm_component_error.assert_called_once_with(generic_runtime_syncer, "test")
