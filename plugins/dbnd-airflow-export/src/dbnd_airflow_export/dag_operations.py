import logging

from airflow.models import DagModel
from airflow.utils.db import provide_session
from sqlalchemy.orm import joinedload

from dbnd._core.constants import AD_HOC_DAG_PREFIX
from dbnd_airflow_export.helpers import _get_git_status
from dbnd_airflow_export.metrics import measure_time, save_result_size
from dbnd_airflow_export.models import EDag


logger = logging.getLogger(__name__)


current_dags = {}


@save_result_size("dags")
@measure_time
def get_dags(
    dagbag, include_task_args, dag_ids, raw_data_only=False, include_sources=True
):
    dag_models = [d for d in current_dags.values() if d]
    if dag_ids is not None:
        dag_models = [dag for dag in dag_models if dag.dag_id in dag_ids]

    number_of_dags_not_in_dag_bag = 0
    dags_list = []
    git_commit, is_committed = _get_git_status(dagbag.dag_folder)

    for dag_model in dag_models:
        try:
            dag_from_dag_bag = dagbag.get_dag(dag_model.dag_id)
        except Exception:
            logger.debug("DAG %s not in a dagbag", dag_model.dag_id)
            dag_from_dag_bag = None

        if dag_from_dag_bag:
            dag = EDag.from_dag(
                dag_from_dag_bag,
                dag_model,
                dagbag.dag_folder,
                include_task_args,
                git_commit,
                is_committed,
                raw_data_only,
                include_sources,
            )
        else:
            dag = EDag.from_dag(
                dag_model,
                dag_model,
                dagbag.dag_folder,
                include_task_args,
                git_commit,
                is_committed,
                raw_data_only,
                include_sources,
            )
            number_of_dags_not_in_dag_bag += 1
        dags_list.append(dag)

    if number_of_dags_not_in_dag_bag > 0:
        logger.info("Found %d dags not in dagbag", number_of_dags_not_in_dag_bag)
    return dags_list


def _dag_query(session):
    dag_models = session.query(DagModel)
    if hasattr(DagModel, "tags"):
        # For backward compatibility with AF < 1.10.8
        dag_models = dag_models.options(joinedload(DagModel.tags))
    return dag_models


@save_result_size("current_dags")
@measure_time
def load_dags_models(session):
    dag_models = _dag_query(session)

    for dag_model in dag_models.all():
        # Exclude dbnd-run tagged runs
        if not dag_model.dag_id.startswith(AD_HOC_DAG_PREFIX):
            current_dags[dag_model.dag_id] = dag_model

    logger.info("Collected %d dags", len(current_dags))
    return current_dags


@measure_time
@provide_session
def get_current_dag_model(dag_id, session=None):
    # MONKEY PATCH for old DagModel.get_current to try cache first
    if dag_id not in current_dags:
        current_dags[dag_id] = (
            _dag_query(session).filter(DagModel.dag_id == dag_id).first()
        )

    return current_dags[dag_id]
