# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Optional

from dbnd._core.tracking.airflow_task_context import AirflowTaskContext
from dbnd.providers.spark.dbnd_spark_init import _safe_get_spark_conf


logger = logging.getLogger(__name__)


def try_get_airflow_context_from_spark_conf():
    # type: ()-> Optional[AirflowTaskContext]
    try:
        conf = _safe_get_spark_conf()
        if conf is not None:
            dag_id = conf.get("spark.env.AIRFLOW_CTX_DAG_ID")
            execution_date = conf.get("spark.env.AIRFLOW_CTX_EXECUTION_DATE")
            task_id = conf.get("spark.env.AIRFLOW_CTX_TASK_ID")
            try_number = conf.get("spark.env.AIRFLOW_CTX_TRY_NUMBER")
            airflow_instance_uid = conf.get("spark.env.AIRFLOW_CTX_UID")

            validate_spark_airflow_conf(
                dag_id=dag_id,
                execution_date=execution_date,
                task_id=task_id,
                try_number=try_number,
                airflow_instance_uid=airflow_instance_uid,
            )

            if dag_id and task_id and execution_date:
                return AirflowTaskContext(
                    dag_id=dag_id,
                    execution_date=execution_date,
                    task_id=task_id,
                    try_number=try_number,
                    airflow_instance_uid=airflow_instance_uid,
                )
    except Exception as ex:
        logger.info("Failed to get airflow context info from spark job: %s", ex)

    return None


def validate_spark_airflow_conf(**kwargs):
    if None not in kwargs.values():
        # all airflow context values are provided, nothing to do here
        return True
    if all(v is None for v in kwargs.values()):
        # airflow context is empty, nothing to do here
        return True
    # some values are provided while others are not
    for kw_key, kw_value in kwargs.items():
        if kw_value is None:
            logger.warning(
                f"Airflow context AIRFLOW_{kw_key.upper()} value is missing in the spark env. Please add it to the spark env to ensure proper tracking of the tasks."
            )
    logger.warning(
        "Some airflow context values are missing in the spark env. This can lead to the incorrect task tracking."
    )
    return False
