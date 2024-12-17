# © Copyright Databand.ai, an IBM Company 2022
import logging

from typing import Any, Dict, List, Optional, Tuple

import attr

from airflow_monitor.common.airflow_data import LastSeenValues
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.plugin_metadata import get_plugin_metadata
from dbnd_airflow.utils import get_or_create_airflow_instance_uid
from dbnd_monitor.adapter.adapter import (
    Assets,
    AssetState,
    AssetToState,
    MonitorAdapter,
    ThirdPartyInfo,
)


logger = logging.getLogger(__name__)


def get_asset_state_from_dag_run(dag_run: Optional[Dict[str, Any]]) -> AssetState:
    if not dag_run:
        return AssetState.FAILED_REQUEST

    dag_run_state = dag_run["state"].lower()

    if dag_run_state == "running" or dag_run_state == "queued":
        return AssetState.ACTIVE
    return AssetState.FINISHED


def update_assets_states(
    assets_to_state: List[AssetToState], dag_runs: List[Dict[str, Any]]
) -> List[AssetToState]:
    dag_run_states = {dag_run["dagrun_id"]: dag_run for dag_run in dag_runs}
    mew_assets_to_state = []

    for asset in assets_to_state:
        dag_run_id = int(asset.asset_id)
        dag_run = dag_run_states.get(dag_run_id)
        new_asset_state = get_asset_state_from_dag_run(dag_run)
        asset = attr.evolve(asset, state=new_asset_state)
        mew_assets_to_state.append(asset)

    return mew_assets_to_state


def get_to_dag_run_ids_from_assets_to_state(
    assets_to_state: List[AssetToState],
) -> List[int]:
    return list(map(lambda asset: int(asset.asset_id), assets_to_state))


class AirflowAdapter(MonitorAdapter[LastSeenValues]):
    def __init__(self, config: AirflowIntegrationConfig):
        self.config = config
        self.db_fetcher = DbFetcher(config)
        self.airflow_instance_uid = get_or_create_airflow_instance_uid()

    def init_cursor(self) -> LastSeenValues:
        return self.db_fetcher.get_last_seen_values()

    def get_new_assets_for_cursor(
        self, cursor: LastSeenValues, active_assets: Optional[Assets] = None
    ) -> Tuple[Assets, LastSeenValues]:
        airflow_response = self.db_fetcher.get_airflow_dagruns_to_sync(
            last_seen_dag_run_id=cursor.last_seen_dag_run_id,
            extra_dag_run_ids=[],
            dag_ids=self.config.dag_ids,
            excluded_dag_ids=self.config.excluded_dag_ids,
        )

        valid_last_seen_dag_run_id = cursor.last_seen_dag_run_id or 0

        assets_to_state = [
            AssetToState(asset_id=str(dag_run.id))
            for dag_run in airflow_response.dag_runs
            if dag_run.id > valid_last_seen_dag_run_id
        ]

        assets = Assets(assets_to_state=assets_to_state)
        cursor = LastSeenValues(airflow_response.last_seen_dag_run_id)

        return assets, cursor

    def get_assets_data(self, assets: Assets) -> Assets:
        if not assets.assets_to_state:
            return Assets()

        dag_run_ids = [int(asset.asset_id) for asset in assets.assets_to_state]
        asset_states = {asset.state for asset in assets.assets_to_state}

        if AssetState.INIT in asset_states:
            dag_runs_data = self.db_fetcher.get_full_dag_runs(
                dag_run_ids, self.config.include_sources
            )
            logger.info("Data for new dag runs %s are fetched", str(dag_run_ids))
        else:
            dag_runs_data = self.db_fetcher.get_dag_runs_state_data(dag_run_ids)
            logger.info("Data for dag runs to update %s are fetched", str(dag_run_ids))

        assets_data = dag_runs_data.as_dict()
        assets_data["airflow_instance_uid"] = self.airflow_instance_uid
        assets_to_state = update_assets_states(
            assets.assets_to_state, dag_runs_data.dag_runs
        )

        return Assets(assets_data, assets_to_state)

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        metadata = get_plugin_metadata()
        metadata_dict = metadata.as_safe_dict() if metadata else {}

        return ThirdPartyInfo(metadata=metadata_dict, error_list=[])
