# © Copyright Databand.ai, an IBM Company 2022

import pytest

from mock import patch

from dbnd import dbnd_tracking_stop
from dbnd._core.configuration.environ_config import (
    get_dbnd_project_config,
    reset_dbnd_project_config,
)
from dbnd._core.tracking.airflow_dag_inplace_tracking import AirflowTaskContext
from dbnd._core.utils.timezone import utcnow


@pytest.fixture
def set_tracking_context():
    try:
        reset_dbnd_project_config()
        get_dbnd_project_config()._dbnd_tracking = True
        yield
    finally:
        dbnd_tracking_stop()
        reset_dbnd_project_config()


@pytest.fixture
def set_airflow_context():
    with patch(
        "dbnd._core.tracking.airflow_dag_inplace_tracking.try_get_airflow_context"
    ) as m:
        try:
            reset_dbnd_project_config()

            m.return_value = AirflowTaskContext(
                dag_id="test_dag",
                task_id="test_task",
                execution_date=utcnow().isoformat(),
            )
            yield
        finally:
            # ensure dbnd_run_stop() is called (normally should happen on exit() )
            dbnd_tracking_stop()
            reset_dbnd_project_config()
