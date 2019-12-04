import logging

from airflow.models import DAG
from airflow.utils.db import provide_session

logger = logging.getLogger(__name__)


class DatabandDAG(DAG):
    # Override create_dagrun so we can create wrapped dagrun that will provide full tracking to dbnd
    @provide_session
    def create_dagrun(
        self,
        run_id,
        state,
        execution_date=None,
        start_date=None,
        external_trigger=False,
        conf=None,
        session=None,
    ):
        from dbnd._core.current import try_get_databand_context
        from dbnd_airflow.dbnd_task_executor.dbnd_dagrun import (
            create_trackable_dagrun_from_af_dag,
        )
        from dbnd_airflow.config import AirflowFeaturesConfig
        if try_get_databand_context() and AirflowFeaturesConfig().track_airflow_dag:
            # this one is called from SchedulerJob in order to "submit" Run for specific day
            #  (it will not be called from manual submission
            return create_trackable_dagrun_from_af_dag(
                run_id=run_id,
                af_dag=self,
                execution_date=execution_date,
                session=session,
                state=state,
                external_trigger=external_trigger,
                conf=conf,
            )
        return super(DatabandDAG, self).create_dagrun(
            run_id=run_id,
            state=state,
            execution_date=execution_date,
            start_date=start_date,
            external_trigger=external_trigger,
            conf=conf,
            session=session,
        )
