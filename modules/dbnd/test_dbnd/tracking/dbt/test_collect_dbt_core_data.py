# © Copyright Databand.ai, an IBM Company 2022

import json

from pathlib import Path

import pytest

from dbnd._core.tracking.dbt import DbtCoreAssets, _extract_environment


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

        [run_step] = result_metadata["run_steps"]

        expected_keys = {
            "manifest",
            "logs",
            "run_results",
            "duration",
            "created_at",
            "started_at",
            "finished_at",
        }
        assert expected_keys.issubset(run_step.keys())

        assert run_step["index"] == 1


class TestExtractFromRunCommand(TestDBTCoreExtractMetaData):
    ASSETS_JSON = "dbt_run_command_assets.json"

    def test_extract_metadata(self, command_assets):
        result_metadata = command_assets.extract_metadata()

        self.validate_result_metadata(result_metadata)
        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["run_steps"][0]["name"] == "dbt run"


class TestExtractFromTestCommand(TestDBTCoreExtractMetaData):
    ASSETS_JSON = "dbt_test_command_assets.json"

    def test_extract_metadata(self, command_assets):
        result_metadata = command_assets.extract_metadata()
        self.validate_result_metadata(result_metadata)
        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["run_steps"][0]["name"] == "dbt test"


class TestExtractFromBuildCommand(TestDBTCoreExtractMetaData):
    ASSETS_JSON = "dbt_build_command_assets.json"

    def test_extract_metadata(self, command_assets):
        result_metadata = command_assets.extract_metadata()
        self.validate_result_metadata(result_metadata)
        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["run_steps"][0]["name"] == "dbt build"


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
