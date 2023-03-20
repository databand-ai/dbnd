# © Copyright Databand.ai, an IBM Company 2022
from typing import Generator, Tuple
from urllib.parse import parse_qsl, urlparse

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    DataStageAssetsClient,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import format_datetime

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetState,
    AssetToState,
)
from dbnd._core.utils.date_utils import parse_datetime
from dbnd._core.utils.timezone import utcnow


FINISHED_RUN_STATES = [
    "finished",
    "success",
    "completed",
    "completedwithwarnings",
    "failed",
    "canceled",
]


class DataStageAdapter(Adapter):
    def __init__(
        self,
        config: DataStageServerConfig,
        datastage_assets_client: DataStageAssetsClient,
    ):
        super(DataStageAdapter, self).__init__(config)
        self.datastage_asset_client = datastage_assets_client

    def init_cursor(self) -> str:
        return format_datetime(utcnow())

    def init_assets_for_cursor(
        self, cursor: str, batch_size: int
    ) -> Generator[Tuple[Assets, str], None, None]:
        new_runs = None
        next_page = None
        start_date = parse_datetime(cursor)
        end_date = format_datetime(utcnow())
        self.datastage_asset_client.client.page_size = batch_size
        while True:
            assets_to_state = []
            new_runs, next_page = self.datastage_asset_client.get_new_runs(
                start_time=format_datetime(start_date),
                end_time=end_date,
                next_page=next_page,
            )
            if new_runs:
                for new_run in new_runs.values():
                    assets_to_state.append(
                        AssetToState(asset_id=new_run, state=AssetState.INIT)
                    )
            yield (Assets(data=None, assets_to_state=assets_to_state), end_date)
            if next_page == None:
                break

    def get_assets_data(self, assets: Assets) -> Assets:
        assets_to_state = assets.assets_to_state
        full_runs = None
        failed_runs = None
        failed_to_retries = {}
        runs_to_update = []
        new_assets_to_state = []
        for asset_to_state in assets_to_state:
            if asset_to_state.state == AssetState.FAILED_REQUEST:
                failed_to_retries[asset_to_state.asset_id] = asset_to_state.retry_count
            run_link = asset_to_state.asset_id
            runs_to_update.append(run_link)
        full_runs, failed_runs = self.datastage_asset_client.get_full_runs(
            runs_to_update
        )
        if full_runs and full_runs.get("runs"):
            for run in full_runs.get("runs"):
                if (
                    run
                    and run.get("run_info", {}).get("state") not in FINISHED_RUN_STATES
                ):
                    new_assets_to_state.append(
                        AssetToState(
                            asset_id=run.get("run_link"), state=AssetState.ACTIVE
                        )
                    )
                # all run states besides running are completed
                else:
                    new_assets_to_state.append(
                        AssetToState(
                            asset_id=run.get("run_link"), state=AssetState.FINISHED
                        )
                    )
        if failed_runs:
            for failed_run in failed_runs:
                new_assets_to_state.append(
                    AssetToState(
                        asset_id=failed_run,
                        state=AssetState.FAILED_REQUEST,
                        retry_count=failed_to_retries.get(failed_run, 0),
                    )
                )
        return Assets(data=full_runs, assets_to_state=new_assets_to_state)

    def _extract_project_id_from_url(self, url: str):
        parsed_url = urlparse(url)
        parsed_query_string = dict(parse_qsl(parsed_url.query))
        project_id = parsed_query_string.get("project_id")
        return project_id
