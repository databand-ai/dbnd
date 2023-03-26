# © Copyright Databand.ai, an IBM Company 2022

import random
import string

from datetime import timedelta

import pytest

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago
from mock import patch

from dbnd_airflow.compat import AIRFLOW_VERSION_2
from dbnd_airflow.tracking.wrap_operators import (
    get_airflow_operator_handlers_config,
    wrap_operator_with_tracking_info,
)


if AIRFLOW_VERSION_2:
    from airflow.providers.amazon.aws.operators.ecs import (
        ECSOperator as AirflowECSOperator,
    )
else:
    from airflow.contrib.operators.ecs_operator import ECSOperator as AirflowECSOperator


class ECSOperatorInhereted(AirflowECSOperator):
    pass


class ECSOperator(BaseOperator):
    """
    This is example of operator that is not from airflow.providers.amazon.aws.operators.ecs
    But has the similar name
    """


def create_ecs_operator(cls, dag):
    return cls(
        task_id="ecs_task_fake" + random_string(8),
        dag=dag,
        cluster="cluster",
        task_definition="definition",
        overrides={},
    )


@pytest.fixture
def dag():
    args = dict(start_date=days_ago(2))
    dag_object = DAG(
        dag_id="test_dag", default_args=args, schedule_interval=timedelta(minutes=1)
    )
    return dag_object


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def test_wrap_operator_regular_should_be_wrapped(dag):
    my_operator = create_ecs_operator(AirflowECSOperator, dag)

    actual = wrap_operator_with_tracking_info(
        {}, my_operator, get_airflow_operator_handlers_config()
    )
    assert actual is not None, "Expected the operator to be wrapped, but it was not"


def test_wrap_operator_inhereted_from_esc_operator(dag):
    my_operator = create_ecs_operator(ECSOperatorInhereted, dag)

    actual = wrap_operator_with_tracking_info(
        {}, my_operator, get_airflow_operator_handlers_config()
    )
    assert actual is not None, "Expected the operator to be wrapped, but it was not"


def test_wrap_operator_not_wrapped_when_module_is_not_from_airflow(dag):
    # This is not real ECSOperator, but it has the same name
    my_operator = ECSOperator(dag=dag, task_id="ecs_task_old" + random_string(8))

    actual = wrap_operator_with_tracking_info(
        {}, my_operator, get_airflow_operator_handlers_config()
    )
    assert (
        actual is None
    ), "Expected the operator not to be wrapped, because it's not from airflow, but it was"


@patch("dbnd_airflow.tracking.wrap_operators.load_python_callable")
def test_user_defined_airflow_operator_handler_should_be_tracked(
    mock_load_python_callable, dag
):
    my_operator = ECSOperator(dag=dag, task_id="ecs_task_old" + random_string(8))
    custom_handler_name = "some_module.custom_handler_name"
    additional_handlers = {
        f"{ECSOperator.__module__}.{ECSOperator.__qualname__}": custom_handler_name
    }

    actual = wrap_operator_with_tracking_info(
        {}, my_operator, get_airflow_operator_handlers_config(additional_handlers)
    )

    assert actual is not None, "Expected custom operator to be wrapped, but it was not"
    mock_load_python_callable.assert_called_once_with(custom_handler_name)


@patch("dbnd_airflow.tracking.wrap_operators.load_python_callable")
def test_user_defined_airflow_operator_via_short_name_should_be_tracked(
    mock_load_python_callable, dag
):
    my_operator = ECSOperator(dag=dag, task_id="ecs_task_old" + random_string(8))
    custom_handler_name = "some_module.custom_handler_name"
    additional_handlers = {f"{ECSOperator.__qualname__}": custom_handler_name}

    actual = wrap_operator_with_tracking_info(
        {}, my_operator, get_airflow_operator_handlers_config(additional_handlers)
    )

    assert actual is not None, "Expected custom operator to be wrapped, but it was not"
    mock_load_python_callable.assert_called_once_with(custom_handler_name)
