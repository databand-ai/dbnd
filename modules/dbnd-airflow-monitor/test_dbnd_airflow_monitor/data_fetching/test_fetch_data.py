import os

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.data_fetchers import DbFetcher

from test_dbnd_airflow_monitor.airflow_utils import airflow_init_db


class TestFetchData(object):
    def test_empty_data(self, empty_db):
        result = self._fetch_data(empty_db)

        assert result is not None
        result_keys = result.keys()

        # validate all fields
        self._validate_keys(result_keys)

        # validate emptiness
        assert len(result["dags"]) == 0
        assert len(result["dag_runs"]) == 0
        assert len(result["task_instances"]) == 0

    def test_sanity(self, unittests_db):
        result = self._fetch_data(unittests_db)

        assert result is not None
        result_keys = result.keys()

        # validate all fields
        self._validate_keys(result_keys)

        # validate dags
        assert len(result["dags"]) >= 1

        # validate dag runs
        assert len(result["dag_runs"]) == 2
        dag_run1 = result["dag_runs"][0]
        dag_run2 = result["dag_runs"][1]
        assert dag_run1["dag_id"] == "tutorial"
        assert dag_run1["state"] == "success"
        assert dag_run2["dag_id"] == "tutorial"
        assert dag_run2["state"] == "success"
        assert (dag_run1["dagrun_id"] == 1 and dag_run2["dagrun_id"] == 2) or (
            dag_run1["dagrun_id"] == 2 and dag_run2["dagrun_id"] == 1
        )

        # validate task instances
        assert len(result["task_instances"]) == 6
        for task_instance in result["task_instances"]:
            assert task_instance["dag_id"] == "tutorial"
            assert task_instance["state"] == "success"
            assert task_instance["task_id"] in ("print_date", "sleep", "templated")
            assert task_instance["try_number"] == 1

    def test_incomplete_data(self, incomplete_data_db):
        result = self._fetch_data(incomplete_data_db)
        assert result is not None
        self._validate_keys(result.keys())

        assert len(result["task_instances"]) == 0

        result = self._fetch_data(incomplete_data_db, incomplete_offset=0)
        assert result is not None
        self._validate_keys(result.keys())

        # 14 runs each with 3 tasks
        assert len(result["dag_runs"]) == 14
        assert len(result["task_instances"]) == 42

        task_instances_without_end_dates = [
            t for t in result["task_instances"] if t["end_date"] is None
        ]
        assert len(task_instances_without_end_dates) == len(result["task_instances"])

    def _fetch_data(self, db_name, quantity=100, incomplete_offset=None):
        db_path = "sqlite:///" + os.path.abspath(
            os.path.normpath(
                os.path.join(os.path.join(os.path.dirname(__file__), db_name))
            )
        )
        airflow_init_db(db_path)

        config = AirflowMonitorConfig()
        config.sql_alchemy_conn = db_path

        fetcher = DbFetcher(config)
        result = fetcher.get_data(
            since="01/09/2020 10:00:00",
            include_logs=True,
            include_task_args=True,
            dag_ids=None,
            quantity=quantity,
            include_xcom=True,
            incomplete_offset=incomplete_offset,
            dags_only=False,
        )

        return result

    def _validate_keys(self, result_keys):
        assert "dags" in result_keys
        assert "dag_runs" in result_keys
        assert "task_instances" in result_keys
        assert "since" in result_keys
        assert "airflow_version" in result_keys
        assert "dags_path" in result_keys
        assert "logs_path" in result_keys
        assert "airflow_export_version" in result_keys
        assert "rbac_enabled" in result_keys
