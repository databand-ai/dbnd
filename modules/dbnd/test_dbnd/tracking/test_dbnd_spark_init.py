# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import patch

from dbnd._core.tracking.dbnd_spark_init import (
    _safe_get_active_spark_context,
    _safe_get_spark_conf,
    validate_spark_airflow_conf,
)


@patch(
    "dbnd._core.tracking.dbnd_spark_init.verify_spark_pre_conditions",
    return_value=False,
)
def test_safe_get_spark_context_no_jvm_no_pre_conditions(verify_spark_pre_conditions):
    spark = _safe_get_active_spark_context()
    assert spark is None


@patch(
    "dbnd._core.tracking.dbnd_spark_init.verify_spark_pre_conditions", return_value=True
)
def test_safe_get_spark_context_no_jvm_with_pre_conditions(verify_spark_pre_conditions):
    spark = _safe_get_active_spark_context()
    assert spark is None


@patch(
    "dbnd._core.tracking.dbnd_spark_init.verify_spark_pre_conditions", return_value=True
)
def test_safe_get_spark_conf_no_jvm_with_pre_conditions(verify_spark_pre_conditions):
    spark_conf = _safe_get_spark_conf()
    assert spark_conf is None


@patch(
    "dbnd._core.tracking.dbnd_spark_init.verify_spark_pre_conditions",
    return_value=False,
)
def test_safe_get_spark_conf_no_jvm_no_pre_conditions(verify_spark_pre_conditions):
    spark_conf = _safe_get_spark_conf()
    assert spark_conf is None


def test_validate_spark_airflow_conf():
    assert (
        validate_spark_airflow_conf(
            dag_id="dag_id",
            execution_date="execution_date",
            task_id="task_id",
            try_number=1,
            airflow_instance_uid="airflow_instance_uid",
        )
        is True
    )
    assert (
        validate_spark_airflow_conf(
            dag_id=None,
            execution_date=None,
            task_id=None,
            try_number=None,
            airflow_instance_uid=None,
        )
        is True
    )
    assert (
        validate_spark_airflow_conf(
            dag_id="dag_id",
            execution_date="execution_date",
            task_id="task_id",
            try_number=None,
            airflow_instance_uid="airflow_instance_uid",
        )
        is False
    )
    assert (
        validate_spark_airflow_conf(
            dag_id="dag_id",
            execution_date="execution_date",
            task_id=None,
            try_number=None,
            airflow_instance_uid="airflow_instance_uid",
        )
        is False
    )
