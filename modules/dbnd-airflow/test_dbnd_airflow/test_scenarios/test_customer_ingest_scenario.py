import pytest

from dbnd_test_scenarios.airflow_scenarios.client_scoring.ingest_data import (
    customer_ingest_dag,
)
from test_dbnd_airflow.functional.utils import run_and_get


class TestCustomerIngestScenario(object):
    @pytest.mark.skip("fixes in context")
    def test_customer_dag_run(self):
        actual = run_and_get(customer_ingest_dag, "create_report")
        assert "create_report" in actual
