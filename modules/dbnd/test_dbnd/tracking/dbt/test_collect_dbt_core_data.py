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
            "connection": {"hostname": "bigquery://dbnd-dev-260010", "type": "bigquery"}
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
            {
                "connection": {
                    "type": "snowflake",
                    "hostname": "snowflake://snowflake-account",
                }
            },
        ),
        (
            # Adapter.REDSHIFT,
            {"type": "redshift", "host": "redshift-host", "port": 8080},
            {
                "connection": {
                    "type": "redshift",
                    "hostname": "redshift://redshift-host:8080",
                }
            },
        ),
        (
            # Adapter.BIGQUERY,
            {"type": "bigquery", "project": "account-dev-5646"},
            {
                "connection": {
                    "type": "bigquery",
                    "hostname": "bigquery://account-dev-5646",
                }
            },
        ),
        # spark
        ## unknown method but with port
        (
            {"type": "spark", "host": "spark-host", "port": 10},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:10"}},
        ),
        (
            {"type": "spark", "host": "spark-host", "method": "unknown", "port": 10},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:10"}},
        ),
        ## unknown methods w/wo ports
        (
            {"type": "spark", "host": "spark-host", "method": "odbc", "port": 10},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:10"}},
        ),
        (
            {"type": "spark", "host": "spark-host", "method": "odbc"},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:443"}},
        ),
        (
            {"type": "spark", "host": "spark-host", "method": "thrift", "port": "321"},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:321"}},
        ),
        (
            {"type": "spark", "host": "spark-host", "method": "thrift"},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:10001"}},
        ),
        (
            {"type": "spark", "host": "spark-host", "method": "http", "port": 12},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:12"}},
        ),
        (
            {"type": "spark", "host": "spark-host", "method": "http"},
            {"connection": {"type": "spark", "hostname": "spark://spark-host:443"}},
        ),
    ],
)
def test_extract_environment(profile, expected):
    profile = {"target": "dev", "outputs": {"dev": profile}}
    assert _extract_environment({"args": {}}, profile) == expected


@pytest.mark.parametrize(
    "profile",
    [
        {"type": "spark", "host": "spark-host", "method": "unknown method"},
        {"type": "no-type"},
    ],
)
def test_failing_extract_environment(profile):
    profile = {"target": "dev", "outputs": {"dev": profile}}
    with pytest.raises(KeyError):
        _extract_environment({"args": {}}, profile)
