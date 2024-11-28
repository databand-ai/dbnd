# © Copyright Databand.ai, an IBM Company 2022
import datetime
import logging
import sys

from typing import List

from airflow_monitor.common.airflow_data import (
    AirflowDagRun,
    AirflowDagRunsResponse,
    DagRunsStateData,
)
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.plugin_metadata import get_plugin_metadata
from airflow_monitor.tracking_service.airflow_tracking_service import (
    AirflowTrackingService,
)
from dbnd._core.utils.timezone import utcnow
from dbnd_monitor.base_component import BaseComponent


logger = logging.getLogger(__name__)


def categorize_dag_runs(
    airflow_dag_runs: List[AirflowDagRun], dbnd_dag_run_ids: List[int]
):
    to_init = []
    to_update = []

    for dr in airflow_dag_runs:
        (to_update if dr.id in dbnd_dag_run_ids else to_init).append(dr)

    return to_init, to_update


class AirflowRuntimeSyncer(BaseComponent):
    SYNCER_TYPE = "runtime_syncer"

    tracking_service: AirflowTrackingService
    config: AirflowIntegrationConfig
    data_fetcher: DbFetcher

    last_success_sync: datetime.datetime = None
    last_sync_heartbeat: datetime.datetime = None

    def _sync_once(self):
        if (
            self.last_success_sync
            and self.config.restart_after_not_synced_minutes
            and self.last_sync_heartbeat - self.last_success_sync
            > datetime.timedelta(minutes=self.config.restart_after_not_synced_minutes)
        ):
            # this mechanism works only after first successful attempt, hoping that
            # restart will help. If we didn't had even one successful attempt - the
            # probability that restart will help is small
            logger.fatal(
                "Didn't sync data for last %s minutes, restarting",
                self.config.restart_after_not_synced_minutes,
            )
            # unfortunately, currently no easy way to restart monitor from _sync_once
            sys.exit(42)

        self.last_sync_heartbeat = utcnow()

        synced_new_data = self._actual_sync_once()

        self.last_success_sync = utcnow()
        # update last_sync_heartbeat as well to prevent false positives for cases
        # when iteration takes too much time
        self.last_sync_heartbeat = self.last_success_sync

        self.reporting_service.report_monitor_time_data(
            self.config.uid, synced_new_data=synced_new_data
        )

    def _actual_sync_once(self):
        dbnd_response = self.tracking_service.get_active_dag_runs(
            start_time_window=self.config.start_time_window,
            dag_ids=self.config.dag_ids,
            excluded_dag_ids=self.config.excluded_dag_ids,
        )
        if dbnd_response.last_seen_dag_run_id is None:
            last_seen_values = self.data_fetcher.get_last_seen_values()
            if last_seen_values.last_seen_dag_run_id is None:
                last_seen_values.last_seen_dag_run_id = -1

            self.tracking_service.update_last_seen_values(last_seen_values)

            dbnd_response = self.tracking_service.get_active_dag_runs(
                start_time_window=self.config.start_time_window,
                dag_ids=self.config.dag_ids,
                excluded_dag_ids=self.config.excluded_dag_ids,
            )

        logger.debug(
            "Getting new dag runs from Airflow with parameters last_seen_dag_run_id=%s, extra_dag_run_ids=%s, dag_ids=%s, excluded_dag_ids=%s",
            dbnd_response.last_seen_dag_run_id,
            ",".join(map(str, dbnd_response.dag_run_ids)),
            self.config.dag_ids,
            self.config.excluded_dag_ids,
        )
        airflow_response = self.data_fetcher.get_airflow_dagruns_to_sync(
            last_seen_dag_run_id=dbnd_response.last_seen_dag_run_id,
            extra_dag_run_ids=dbnd_response.dag_run_ids,
            dag_ids=self.config.dag_ids,
            excluded_dag_ids=self.config.excluded_dag_ids,
        )  # type: AirflowDagRunsResponse

        dagruns_to_init, dagruns_to_update = categorize_dag_runs(
            airflow_response.dag_runs, dbnd_response.dag_run_ids
        )

        self.init_runs(dagruns_to_init)
        self.update_runs(dagruns_to_update)

        synced_new_data = len(dagruns_to_init) > 0 or len(dagruns_to_update) > 0
        return synced_new_data

    def init_runs(self, run_ids_to_init: List[AirflowDagRun]):
        if not run_ids_to_init:
            return

        plugin_metadata = get_plugin_metadata()
        dagruns: List[AirflowDagRun] = sorted(run_ids_to_init, key=lambda dr: dr.id)

        bulk_size = self.config.dag_run_bulk_size or len(dagruns)
        for i in range(0, len(dagruns), bulk_size):
            dagruns_chunk = dagruns[i : i + bulk_size]
            dag_run_ids = [dr.id for dr in dagruns_chunk]
            dag_runs_full_data = self.data_fetcher.get_full_dag_runs(
                dag_run_ids, self.config.include_sources
            )
            logger.info(
                "Syncing new %d dag runs and %d task instances",
                len(dag_runs_full_data.dag_runs),
                len(dag_runs_full_data.task_instances),
            )
            self.tracking_service.init_dagruns(
                dag_runs_full_data, max(dag_run_ids), self.SYNCER_TYPE, plugin_metadata
            )

    def update_runs(self, run_ids_to_update: List[AirflowDagRun]):
        # update_dagruns will fetch updated states for given dagruns and send them
        # to webserver (in chunks). As a side effect - webserver will also update
        # last_sync_time which is used as heartbeat. We do it as part of update_dagruns
        # to be sure that we update last_sync_time only if data is being updated.
        if not run_ids_to_update:
            # if there is no dagruns to update - do empty update to set last_sync_time
            # otherwise we might get false alarms that monitor is not syncing
            self.tracking_service.update_dagruns(
                DagRunsStateData(task_instances=[], dag_runs=[]), self.SYNCER_TYPE
            )
            return

        bulk_size = self.config.dag_run_bulk_size or len(run_ids_to_update)
        for i in range(0, len(run_ids_to_update), bulk_size):
            dagruns_chunk = run_ids_to_update[i : i + bulk_size]
            dag_run_ids = [dr.id for dr in dagruns_chunk]
            dag_runs_state_data = self.data_fetcher.get_dag_runs_state_data(dag_run_ids)
            logger.info(
                "Updating states for %d dag runs and %d task instances",
                len(dag_runs_state_data.dag_runs),
                len(dag_runs_state_data.task_instances),
            )

            self.tracking_service.update_dagruns(dag_runs_state_data, self.SYNCER_TYPE)
