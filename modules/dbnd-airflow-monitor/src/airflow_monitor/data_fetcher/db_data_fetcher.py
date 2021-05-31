import contextlib
import json
import logging

from distutils.version import LooseVersion
from typing import List, Optional

from airflow_monitor.common.airflow_data import (
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
    PluginMetadata,
)
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from dbnd._core.utils.uid_utils import get_airflow_instance_uid


logger = logging.getLogger(__name__)


def json_conv(data):
    from dbnd_airflow_export.utils import JsonEncoder

    if not isinstance(data, dict):
        data = data.as_dict()

    return json.loads(json.dumps(data, cls=JsonEncoder))


class DbFetcher(AirflowDataFetcher):
    def __init__(self, config):
        # type: (AirflowServerConfig) -> DbFetcher
        super(DbFetcher, self).__init__(config)
        # It's important to do this import to prevent import issues
        import airflow
        from sqlalchemy import create_engine

        self.dag_folder = config.local_dag_folder
        self.sql_conn_string = config.sql_alchemy_conn
        self.engine = create_engine(self.sql_conn_string)
        self.env = "AirflowDB"

        self._engine = None
        self._session = None
        self._dagbag = None

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

    def _get_dagbag(self):
        if not self._dagbag:
            from airflow import models, settings

            if hasattr(settings, "STORE_SERIALIZED_DAGS"):
                from airflow.settings import STORE_SERIALIZED_DAGS

                self._dagbag = models.DagBag(
                    self.dag_folder if self.dag_folder else settings.DAGS_FOLDER,
                    include_examples=True,
                    store_serialized_dags=STORE_SERIALIZED_DAGS,
                )
            else:
                self._dagbag = models.DagBag(
                    self.dag_folder if self.dag_folder else settings.DAGS_FOLDER,
                    include_examples=True,
                )
        return self._dagbag

    def get_last_seen_values(self) -> LastSeenValues:
        from dbnd_airflow_export.api_functions import get_last_seen_values

        with self._get_session() as session:
            data = get_last_seen_values(session=session)
        return LastSeenValues.from_dict(json_conv(data))

    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int],
        last_seen_log_id: Optional[int],
        extra_dag_run_ids: Optional[List[int]],
        dag_ids: Optional[str],
    ) -> AirflowDagRunsResponse:
        from dbnd_airflow_export.api_functions import get_new_dag_runs

        dag_ids_list = dag_ids.split(",") if dag_ids else None

        with self._get_session() as session:
            data = get_new_dag_runs(
                last_seen_dag_run_id=last_seen_dag_run_id,
                last_seen_log_id=last_seen_log_id,
                extra_dag_runs_ids=extra_dag_run_ids,
                dag_ids=dag_ids_list,
                include_subdags=False,
                session=session,
            )
        return AirflowDagRunsResponse.from_dict(json_conv(data))

    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        from dbnd_airflow_export.api_functions import get_full_dag_runs

        with self._get_session() as session:
            data = get_full_dag_runs(
                dag_run_ids=dag_run_ids,
                include_sources=include_sources,
                airflow_dagbag=self._get_dagbag(),
                session=session,
            )

        return DagRunsFullData.from_dict(json_conv(data))

    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        from dbnd_airflow_export.api_functions import get_dag_runs_states_data

        with self._get_session() as session:
            data = get_dag_runs_states_data(dag_run_ids=dag_run_ids, session=session)

        return DagRunsStateData.from_dict(json_conv(data))

    def is_alive(self):
        return True

    def get_plugin_metadata(self) -> PluginMetadata:
        from airflow import version as airflow_version
        import dbnd_airflow_export

        return PluginMetadata(
            airflow_version=airflow_version.version,
            plugin_version=dbnd_airflow_export.__version__ + " v2",
            airflow_instance_uid=get_airflow_instance_uid(),
        )
