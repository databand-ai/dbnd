# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import json
import logging

from typing import List, Optional

from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.common.metric_reporter import METRIC_REPORTER, measure_time
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.errors import AirflowFetchingException
from dbnd._vendor.tenacity import retry, stop_after_attempt, wait_fixed


logger = logging.getLogger(__name__)


def json_conv(data):
    from dbnd_airflow.export_plugin.utils import JsonEncoder

    if not isinstance(data, dict):
        data = data.as_dict()

    return json.loads(json.dumps(data, cls=JsonEncoder))


class DbFetcher(AirflowDataFetcher):
    def __init__(self, config: AirflowIntegrationConfig) -> None:
        super().__init__(config)
        # It's important to do this import to prevent import issues
        import airflow  # noqa: F401

        from sqlalchemy import create_engine

        from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader

        self.dag_folder = config.local_dag_folder
        self.sql_conn_string = config.sql_alchemy_conn
        self.engine = create_engine(self.sql_conn_string)
        self.env = "AirflowDB"

        self._engine = None
        self._session = None
        # we want to load dags one in the current session
        self._dag_loader = DbndDagLoader()

    @contextlib.contextmanager
    def _get_session(self):
        import airflow

        if hasattr(airflow, "conf"):
            from airflow import conf
        else:
            from airflow.configuration import conf

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        if not self._engine:
            if not conf.has_section("core"):
                logger.info("Adding 'core' section to airflow config.")
                conf.add_section("core")

            conf.set("core", "sql_alchemy_conn", value=self.sql_conn_string)
            self._engine = create_engine(self.sql_conn_string)

            self._session = sessionmaker(bind=self._engine)

        session = self._session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _raise_on_plugin_error_message(self, data, function_name):
        error_message = getattr(data, "error_message", None)
        if error_message:
            if "QueuePool limit" in error_message:
                # sometimes we get this error:
                # sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached, connection timed out, timeout 30
                # let's try to force close all open sessions, and hope it will fix the issue
                from sqlalchemy.orm import close_all_sessions

                close_all_sessions()
            raise AirflowFetchingException(
                f"Exception occurred in function {function_name} in Airflow: {error_message}"
            )

    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1))
    def get_last_seen_values(self) -> LastSeenValues:
        from dbnd_airflow.export_plugin.api_functions import get_last_seen_values

        with self._get_session() as session:
            data = get_last_seen_values(session=session)

            self._raise_on_plugin_error_message(data, "get_last_seen_values")
            json_data = json_conv(data)
        self._on_data_received(json_data, "get_last_seen_values")
        return LastSeenValues.from_dict(json_data)

    @measure_time(metric=METRIC_REPORTER.exporter_response_time, label=__file__)
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1))
    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int] = None,
        last_seen_log_id: Optional[int] = None,
        extra_dag_run_ids: Optional[List[int]] = None,
        dag_ids: Optional[str] = None,
    ) -> AirflowDagRunsResponse:
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs

        dag_ids_list = dag_ids.split(",") if dag_ids else None

        with self._get_session() as session:
            data = get_new_dag_runs(
                last_seen_dag_run_id=last_seen_dag_run_id,
                last_seen_log_id=last_seen_log_id,
                extra_dag_runs_ids=extra_dag_run_ids,
                dag_ids=dag_ids_list,
                include_subdags=True,
                session=session,
            )

            self._raise_on_plugin_error_message(data, "get_airflow_dagruns_to_sync")
            json_data = json_conv(data)
        self._on_data_received(json_data, "get_airflow_dagruns_to_sync")
        return AirflowDagRunsResponse.from_dict(json_data)

    @measure_time(metric=METRIC_REPORTER.exporter_response_time, label=__file__)
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1))
    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs

        with self._get_session() as session:
            # load missing dags
            self._dag_loader.load_dags_for_runs(dag_run_ids, session)

            data = get_full_dag_runs(
                dag_run_ids=dag_run_ids,
                include_sources=include_sources,
                dag_loader=self._dag_loader,
                session=session,
            )

            self._raise_on_plugin_error_message(data, "get_full_dag_runs")
            json_data = json_conv(data)
        self._on_data_received(json_data, "get_full_dag_runs")
        return DagRunsFullData.from_dict(json_data)

    @measure_time(metric=METRIC_REPORTER.exporter_response_time, label=__file__)
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1))
    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        from dbnd_airflow.export_plugin.api_functions import get_dag_runs_states_data

        with self._get_session() as session:
            data = get_dag_runs_states_data(dag_run_ids=dag_run_ids, session=session)

            self._raise_on_plugin_error_message(data, "get_dag_runs_state_data")
            json_data = json_conv(data)
        self._on_data_received(json_data, "get_dag_runs_state_data")
        return DagRunsStateData.from_dict(json_data)

    def is_alive(self):
        return True
