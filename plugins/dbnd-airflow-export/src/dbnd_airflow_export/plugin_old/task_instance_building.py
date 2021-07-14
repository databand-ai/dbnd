import itertools
import logging
import sys

import six

from airflow.models import XCom

from dbnd_airflow_export.plugin_old.helpers import _get_log
from dbnd_airflow_export.plugin_old.metrics import measure_time
from dbnd_airflow_export.plugin_old.model import ETaskInstance


try:
    # in dbnd it might be overridden
    from airflow.models import original_TaskInstance as TaskInstance
except Exception:
    from airflow.models import TaskInstance


MAX_XCOM_SIZE_IN_BYTES = 10000
MAX_XCOM_LENGTH = 10


def build_task_instance(ti_fields, dagbag, include_xcom, include_logs, session):
    eti = ETaskInstance(*ti_fields)
    if include_xcom or include_logs:
        dag_from_dag_bag = dagbag.get_dag(eti.dag_id)
        if dag_from_dag_bag:
            if include_xcom:
                xcom_dict = _get_task_instance_xcom_dict(
                    dag_id=eti.dag_id,
                    task_id=eti.task_id,
                    execution_date=eti.execution_date,
                )
                eti.xcom_dict = xcom_dict
            if include_logs:
                task = (
                    dag_from_dag_bag.get_task(eti.task_id)
                    if dag_from_dag_bag and dag_from_dag_bag.has_task(eti.task_id)
                    else None
                )
                if task:
                    ti = (
                        session.query(TaskInstance)
                        .filter(
                            TaskInstance.task_id == eti.task_id,
                            TaskInstance.dag_id == eti.dag_id,
                            TaskInstance.execution_date == eti.execution_date,
                        )
                        .first()
                    )
                    eti.log_body = _get_log(ti, task)
    return eti


@measure_time
def _get_task_instance_xcom_dict(dag_id, task_id, execution_date):
    try:
        results = XCom.get_many(
            dag_ids=dag_id, task_ids=task_id, execution_date=execution_date
        )
        if not results:
            return {}

        xcom_dict = {xcom.key: str(xcom.value) for xcom in results}

        sliced_xcom = (
            dict(itertools.islice(xcom_dict.items(), MAX_XCOM_LENGTH))
            if len(xcom_dict) > MAX_XCOM_LENGTH
            else xcom_dict
        )
        for key, value in six.iteritems(sliced_xcom):
            sliced_xcom[key] = _shorten_xcom_value(value)

        return sliced_xcom
    except Exception as e:
        logging.info("Failed to get xcom dict. Exception: {}".format(e))
        return {}


def _shorten_xcom_value(xcom_value):
    if sys.getsizeof(xcom_value) <= MAX_XCOM_SIZE_IN_BYTES:
        return xcom_value

    diff = len(xcom_value) - MAX_XCOM_SIZE_IN_BYTES
    return xcom_value[-diff:]
