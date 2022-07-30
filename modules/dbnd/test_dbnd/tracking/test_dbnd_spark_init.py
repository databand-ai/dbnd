# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import patch

from dbnd._core.tracking.dbnd_spark_init import (
    _safe_get_active_spark_context,
    _safe_get_spark_conf,
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
