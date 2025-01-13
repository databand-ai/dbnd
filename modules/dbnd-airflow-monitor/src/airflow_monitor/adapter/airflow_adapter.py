# © Copyright Databand.ai, an IBM Company 2022
import logging

from typing import Any, Dict, List, Optional, Tuple, Union

import attr

from airflow_monitor.common.airflow_data import DagRunsFullData, DagRunsStateData
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from dbnd_monitor.adapter.adapter import (
    Assets,
    AssetState,
    AssetToState,
    MonitorAdapter,
    ThirdPartyInfo,
)


logger = logging.getLogger(__name__)


def get_asset_state_from_dag_run(dag_run: Optional[Dict[str, Any]]) -> AssetState:
    from dbnd_airflow.export_plugin.models import DagRunState

    if not dag_run:
        return AssetState.FAILED_REQUEST

    dag_run_state = dag_run["state"].lower()

    if dag_run_state in (DagRunState.SUCCESS, DagRunState.FAILED):
        return AssetState.FINISHED
    return AssetState.ACTIVE


def update_assets_states(
    assets_to_state: List[AssetToState], dag_runs: List[Dict[str, Any]]
) -> List[AssetToState]:
    dag_run_states = {dag_run["dagrun_id"]: dag_run for dag_run in dag_runs}
    new_assets_to_state = []

    for asset in assets_to_state:
        dag_run_id = int(asset.asset_id)
        dag_run = dag_run_states.get(dag_run_id)
        new_asset_state = get_asset_state_from_dag_run(dag_run)
        asset = attr.evolve(asset, state=new_asset_state)
        new_assets_to_state.append(asset)

    return new_assets_to_state


def split_assets_by_active_state(
    assets_to_state: List[AssetToState],
) -> Tuple[List[AssetToState], List[AssetToState]]:
    active, rest = [], []

    for asset in assets_to_state:
        (active if asset.state == AssetState.ACTIVE else rest).append(asset)

    return active, rest


def merge_dag_runs_full_and_state_data(
    full_data: DagRunsFullData, state_data: DagRunsStateData
) -> Union[DagRunsFullData, DagRunsStateData]:
    dag_runs = full_data.dag_runs + state_data.dag_runs
    task_instances = full_data.task_instances + state_data.task_instances

    if full_data.dags:
        return DagRunsFullData(full_data.dags, dag_runs, task_instances)
    return DagRunsStateData(dag_runs, task_instances)


def get_dag_run_ids_from_assets(assets_to_state: List[AssetToState]) -> List[int]:
    asset_ids = [asset.asset_id for asset in assets_to_state]
    return list(map(int, asset_ids))


class AirflowAdapter(MonitorAdapter[Optional[int]]):
    def __init__(self, config: AirflowIntegrationConfig):
        self.config = config
        self.db_fetcher = DbFetcher(config)
        self.airflow_instance_uid = DbFetcher.get_airflow_instance_uid()

    def init_cursor(self) -> int:
        last_seen_values = self.db_fetcher.get_last_seen_values()
        return last_seen_values.last_seen_dag_run_id

    def get_new_assets_for_cursor(
        self, cursor: Optional[int], active_assets: Optional[Assets] = None
    ) -> Tuple[Assets, Optional[int]]:
        airflow_response = self.db_fetcher.get_airflow_dagruns_to_sync(
            last_seen_dag_run_id=cursor,
            extra_dag_run_ids=[],
            dag_ids=self.config.dag_ids,
            excluded_dag_ids=self.config.excluded_dag_ids,
        )

        asset_ids = {str(dag_run.id) for dag_run in airflow_response.dag_runs}

        # db_fetcher.get_airflow_dagruns_to_sync returns dag runs that have id > cursor or that are running.
        # We might need dag runs with id < cursor that are running when some finished dag run is re-run.
        # This is achieved by filtering by active_asset_ids
        if active_assets:
            active_asset_ids = {
                asset.asset_id for asset in active_assets.assets_to_state
            }
            asset_ids = asset_ids.difference(active_asset_ids)

        assets_to_state = [AssetToState(id) for id in asset_ids]

        assets = Assets(assets_to_state=assets_to_state)
        cursor = airflow_response.last_seen_dag_run_id

        return assets, cursor

    def get_assets_data(self, assets: Assets) -> Assets:
        if not assets.assets_to_state:
            return Assets()

        active_assets, new_assets = split_assets_by_active_state(assets.assets_to_state)

        full_data = self.get_assets_full_data(new_assets)
        state_data = self.get_assets_state_data(active_assets)
        data = merge_dag_runs_full_and_state_data(full_data, state_data)

        assets_data = data.as_dict()
        assets_data["airflow_instance_uid"] = self.airflow_instance_uid
        assets_to_state = update_assets_states(assets.assets_to_state, data.dag_runs)

        return Assets(assets_data, assets_to_state)

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        metadata = self.db_fetcher.get_plugin_metadata()
        metadata_dict = metadata.as_safe_dict() if metadata else {}

        return ThirdPartyInfo(metadata=metadata_dict, error_list=[])

    def get_assets_full_data(
        self, assets_to_state: List[AssetToState]
    ) -> DagRunsFullData:
        dag_run_ids = get_dag_run_ids_from_assets(assets_to_state)
        dag_runs_data = self.db_fetcher.get_full_dag_runs(
            dag_run_ids, self.config.include_sources
        )
        logger.info("Data for new dag runs %s are fetched", str(dag_run_ids))
        return dag_runs_data

    def get_assets_state_data(
        self, assets_to_state: List[AssetToState]
    ) -> DagRunsStateData:
        dag_run_ids = get_dag_run_ids_from_assets(assets_to_state)
        dag_runs_data = self.db_fetcher.get_dag_runs_state_data(dag_run_ids)
        logger.info("Data for dag runs to update %s are fetched", str(dag_run_ids))
        return dag_runs_data
