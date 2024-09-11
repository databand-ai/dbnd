# © Copyright Databand.ai, an IBM Company 2024

import pytest

from dbnd_airflow.raw_constants import MONITOR_DAG_NAME
from dbnd_airflow.tracking.execute_tracking import is_dag_eligible_for_tracking


@pytest.mark.parametrize(
    "dag_id, tracking_list, excluded_tracking_list, expected_result",
    [
        (MONITOR_DAG_NAME, None, None, True),
        (MONITOR_DAG_NAME, [MONITOR_DAG_NAME], None, True),
        (MONITOR_DAG_NAME, None, [MONITOR_DAG_NAME], True),
        ("DAG_1", None, None, True),
        ("DAG_2", ["DAG_1", "DAG_2", "DAG_3"], None, True),
        ("DAG_2.subdag", ["DAG_1", "DAG_2", "DAG_3"], None, True),
        ("DAG_4", ["DAG_1", "DAG_2", "DAG_3"], None, False),
        ("DAG_4.DAG_2", ["DAG_1", "DAG_2", "DAG_3"], None, False),
        ("DAG_4", None, ["DAG_1", "DAG_2", "DAG_3"], True),
        ("DAG_2", None, ["DAG_1", "DAG_2", "DAG_3"], False),
        ("DAG_2.DAG_4", None, ["DAG_1", "DAG_2", "DAG_3"], False),
        ("DAG_5.DAG_3", None, ["DAG_1", "DAG_2", "DAG_3"], True),
        ("DAG_1", ["DAG_1"], ["DAG_1"], True),
        ("DAG_1", ["DAG_2"], ["DAG_2"], False),
    ],
)
def test_is_dag_eligible_for_tracking(
    dag_id, tracking_list, excluded_tracking_list, expected_result
):
    result = is_dag_eligible_for_tracking(dag_id, tracking_list, excluded_tracking_list)
    assert result == expected_result
