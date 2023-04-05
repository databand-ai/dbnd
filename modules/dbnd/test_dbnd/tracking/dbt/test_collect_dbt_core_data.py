# © Copyright Databand.ai, an IBM Company 2022

import json
import os

from pathlib import Path
from unittest import mock
from unittest.mock import mock_open

import dateutil.parser
import pytest

from mock import MagicMock, patch

import dbnd

from dbnd._core.tracking.dbt.dbt_core import (
    DbtCoreAssets,
    _extract_environment,
    _extract_jinja_values,
    _load_dbt_core_assets,
    _read_and_truncate_logs,
    collect_data_from_dbt_core,
)
from dbnd._core.utils.basics.environ_utils import env


class TestDBTCoreExtractMetaData:
    ASSETS_JSON: str

    def _load_dbt_core_assets(self, assets_json):
        p = Path(__file__).with_name(assets_json)
        with p.open("r") as fp:
            assets = DbtCoreAssets(**json.load(fp))
        return assets

    @pytest.fixture()
    def command_assets(self):
        return self._load_dbt_core_assets(self.ASSETS_JSON)

    def validate_result_metadata(self, result_metadata):
        assert result_metadata["environment"] == {
            "connection": {
                "location": "US",
                "project": "dbnd-dev-260010",
                "type": "bigquery",
            }
        }

        expected_keys = {
            "id",
            "job",
            "job_id",
            "started_at",
            "created_at",
            "is_complete",
        }

        assert expected_keys.issubset(result_metadata.keys())

        [run_step] = result_metadata["run_steps"]

        expected_run_step_keys = {
            "manifest",
            "logs",
            "run_results",
            "duration",
            "created_at",
            "started_at",
            "finished_at",
        }
        assert expected_run_step_keys.issubset(run_step.keys())

        assert run_step["index"] == 1

        actual_elapsed_time = (
            dateutil.parser.parse(run_step["finished_at"])
            - dateutil.parser.parse(run_step["created_at"])
        ).total_seconds()
        expected_elapsed_time = run_step["duration"]
        assert abs(expected_elapsed_time - actual_elapsed_time) < 1

        assert any(
            node_value.get("compiled")
            for node_value in run_step["manifest"]["nodes"].values()
        )

        for node_value in run_step["manifest"]["nodes"].values():
            if node_value.get("compiled"):
                assert "compiled_code" in node_value


class TestExtractFromRunCommand(TestDBTCoreExtractMetaData):
    ASSETS_JSON = "dbt_run_command_assets.json"

    def test_extract_metadata(self, command_assets):
        result_metadata = command_assets.extract_metadata()

        self.validate_result_metadata(result_metadata)
        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["run_steps"][0]["name"] == "dbt run"
        assert result_metadata["job"]["name"] == "run"


class TestExtractFromTestCommand(TestDBTCoreExtractMetaData):
    ASSETS_JSON = "dbt_test_command_assets.json"

    def test_extract_metadata(self, command_assets):
        result_metadata = command_assets.extract_metadata()
        self.validate_result_metadata(result_metadata)
        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["run_steps"][0]["name"] == "dbt test"
        assert result_metadata["job"]["name"] == "test"


class TestExtractFromBuildCommand(TestDBTCoreExtractMetaData):
    ASSETS_JSON = "dbt_build_command_assets.json"

    def test_extract_metadata(self, command_assets):
        result_metadata = command_assets.extract_metadata()
        self.validate_result_metadata(result_metadata)
        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["run_steps"][0]["name"] == "dbt build"
        assert result_metadata["job"]["name"] == "build"


@pytest.mark.parametrize(
    "profile, expected",
    [
        (
            # Adapter.SNOWFLAKE,
            {"type": "snowflake", "account": "snowflake-account"},
            {"connection": {"account": "snowflake-account", "type": "snowflake"}},
        ),
        (
            # Adapter.REDSHIFT,
            {"type": "redshift", "host": "redshift-host", "port": 8080},
            {
                "connection": {
                    "host": "redshift-host",
                    "hostname": "redshift-host",
                    "type": "redshift",
                }
            },
        ),
        (
            # Adapter.bigquery,
            {"type": "bigquery", "project": "account-dev-5646", "location": "us"},
            {
                "connection": {
                    "location": "us",
                    "project": "account-dev-5646",
                    "type": "bigquery",
                }
            },
        ),
        # spark
        (
            {"type": "spark", "host": "spark-host", "port": 10},
            {
                "connection": {
                    "host": "spark-host",
                    "hostname": "spark-host",
                    "type": "spark",
                }
            },
        ),
    ],
)
def test_extract_environment(profile, expected):
    profile = {"target": "dev", "outputs": {"dev": profile}}
    assert _extract_environment({"args": {}}, profile) == expected


@patch("dbnd._core.tracking.dbt.dbt_core._load_json_file")
@patch("dbnd._core.tracking.dbt.dbt_core._read_and_truncate_logs")
@patch("dbnd._core.tracking.dbt.dbt_core._load_yaml_with_jinja")
def test_load_dbt_core_assets(
    mock_load_yaml_with_jinja, mock_read_and_truncate_logs, mock_load_json_file
):
    mock_load_json_file.side_effect = [MagicMock(), MagicMock()]
    mock_load_yaml_with_jinja.side_effect = [
        {"profile": "sample_profile"},
        {"sample_profile": MagicMock()},
    ]
    mock_read_and_truncate_logs.return_value = MagicMock()

    assets = _load_dbt_core_assets("fake/path")

    assert isinstance(assets, DbtCoreAssets)


def test_read_and_truncate_logs():
    dbt_project_path = "/dummy/dbt_project_path"
    dbt_log_path = os.path.join(dbt_project_path, "logs", "dbt.log")
    original_log_content = "Some log content"

    m = mock_open(read_data=original_log_content)
    with patch("dbnd._core.tracking.dbt.dbt_core.open", m):
        with patch("dbnd._core.tracking.dbt.dbt_core.shutil.copy") as copy_mock:
            result = _read_and_truncate_logs(dbt_project_path)

    assert (
        result == original_log_content
    ), "The function did not read the log content correctly"

    assert copy_mock.call_count == 1, "A backup of the log file was not created"

    assert m.call_args_list[-1] == mock.call(
        dbt_log_path, "w"
    ), "The original log file was not truncated"


def test_extract_jinja_values():
    values = {
        "env_var_key": "{{ env_var('env_var_identifier') }}",
        "list_value": [
            1,
            2,
            "value",
            {"env_var_key": "{{ env_var('env_var_identifier') }}"},
        ],
        "none_value": None,
        "env_var_number_key": "{{ env_var('env_var_number_key') | as_number }}",
        "jinja_error": "{{ undefined_for_jinja(whatever) }}",
    }
    expected = {
        "env_var_key": "env_var_literal",
        "list_value": [1, 2, "value", {"env_var_key": "env_var_literal"}],
        "none_value": None,
        "env_var_number_key": "2",
        "jinja_error": "{{ undefined_for_jinja(whatever) }}",
    }

    with env(env_var_identifier="env_var_literal", env_var_number_key="2"):
        actual = _extract_jinja_values(values)

    assert expected == actual


def test_extract_jinja_values_with_undefined_env_var_raise_environment_error():
    values = {"env_var_key": "{{ env_var('undefined_env_var_identifier') }}"}
    with pytest.raises(expected_exception=EnvironmentError):
        _extract_jinja_values(values)


def test_dbt_sdk_functions_are_importable_from_top_level():
    assert hasattr(dbnd, "collect_data_from_dbt_core")
    assert hasattr(dbnd, "collect_data_from_dbt_cloud")

    assert callable(dbnd.collect_data_from_dbt_core)
    assert callable(dbnd.collect_data_from_dbt_cloud)


def test_undefined_error_not_called():
    values = {"env_var_key": "{{ env_var('env_var_identifier') }}"}
    with patch("logging.Logger.debug") as mock:
        with env(env_var_identifier="env_var_literal", env_var_number_key="2"):
            _extract_jinja_values(values)
        assert mock.call_count == 0


def test_undefined_error_called():
    values = {"test_jinja_error": "{{ test_undefined_for_jinja(whatever) }}"}
    with patch("logging.Logger.debug") as mock:
        with env(env_var_identifier="env_var_literal", env_var_number_key="2"):
            _extract_jinja_values(values)
        mock.assert_called_with(
            "Jinja template error has occurred: 'test_undefined_for_jinja' is undefined"
        )
        assert mock.called


@patch("dbnd._core.tracking.dbt.dbt_core._get_tracker")
@patch("dbnd._core.tracking.dbt.dbt_core._load_dbt_core_assets")
def test_dbt_core_processing_not_happening_if_no_tracker_is_available(
    load_dbt_core_assets_mock, get_tracker_mock
):
    get_tracker_mock.return_value = None

    dbt_project_path = "dummy_dbt_project_path"
    collect_data_from_dbt_core(dbt_project_path)

    get_tracker_mock.assert_called_once()
    load_dbt_core_assets_mock.assert_not_called()
