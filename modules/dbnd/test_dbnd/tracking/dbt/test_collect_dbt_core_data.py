# © Copyright Databand.ai, an IBM Company 2022

import json

from pathlib import Path

import pytest

from dbnd._core.tracking.dbt import (
    DbtCoreAssets,
    _extract_environment,
    _extract_jinja_values,
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
            # Adapter.BIGQUERY,
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
