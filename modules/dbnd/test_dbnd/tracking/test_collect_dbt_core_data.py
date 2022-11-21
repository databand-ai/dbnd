# © Copyright Databand.ai, an IBM Company 2022

import json

from pathlib import Path

import pytest

from dbnd._core.tracking.dbt import DbtCoreAssets, extract_metadata


class TestDBTCoreExtractMetaData:
    @pytest.fixture
    def run_command_assets(self):
        p = Path(__file__).with_name("dbt_run_command_assets.json")
        with p.open("r") as fp:
            assets = DbtCoreAssets(**json.load(fp))

        return assets

    def test_dbt_run_command(self, run_command_assets):
        result_metadata = extract_metadata(run_command_assets)

        assert result_metadata["status_humanized"] == "pass"
        assert result_metadata["environment"] == {
            "connection": {"type": "bigquery", "hostname": "bigquery"}
        }

        [run_step] = result_metadata["run_steps"]

        assert "manifest" in run_step
        assert "logs" in run_step
        assert "run_results" in run_step

        #  in dbt-core we can collect only one step at a time, so the index must always
        assert run_step["index"] == 1

        assert run_step["duration"] == 13.301862955093384
        assert run_step["created_at"] == "2022-11-17T14:43:09.104063Z"
        assert run_step["started_at"] == "2022-11-17T14:42:57.112968Z"
        assert run_step["finished_at"] == "2022-11-17T14:43:09.098254Z"
        assert run_step["name"] == "dbt test"
