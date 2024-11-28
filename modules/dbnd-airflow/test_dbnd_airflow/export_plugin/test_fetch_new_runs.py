# © Copyright Databand.ai, an IBM Company 2022


from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
from test_dbnd_airflow.export_plugin.db_data_generator import (
    insert_dag_runs,
    set_dag_is_paused,
)


class TestNewRuns:
    def validate_result(
        self, result, expected_dag_runs, expected_max_dag_run_id, is_paused=False
    ):
        assert result
        assert result.error_message is None
        assert len(result.new_dag_runs) == expected_dag_runs
        assert result.last_seen_dag_run_id is expected_max_dag_run_id

        result.new_dag_runs = sorted(result.new_dag_runs, key=lambda r: r.id)

        if result.new_dag_runs:
            paused_runs = [
                new_run for new_run in result.new_dag_runs if new_run.is_paused
            ]
            if is_paused:
                assert len(paused_runs) == expected_dag_runs
            else:
                assert len(paused_runs) == 0

    def test_01_empty_db(self):

        result = get_new_dag_runs(1, [])
        self.validate_result(result, 0, None, None)

    def test_02_both_none(self):
        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(None, [])
        self.validate_result(result, 0, 3)

    def test_03_running(self):
        insert_dag_runs(dag_runs_count=3, state="running", with_log=False)

        result = get_new_dag_runs(None, [])
        self.validate_result(result, 3, 3)

    def test_04_dag_run_id_none(self):
        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(None, [])
        self.validate_result(result, 0, 3)

    def test_06_dag_run_id_0(self):
        insert_dag_runs(with_log=True)

        result = get_new_dag_runs(0, [])
        self.validate_result(result, 1, 1)

    def test_07_big_run_id(self):
        insert_dag_runs(dag_runs_count=3, with_log=False)

        result = get_new_dag_runs(3, [])
        self.validate_result(result, 0, 3)

        assert result

    def test_09_paused(self):
        insert_dag_runs(dag_runs_count=1, with_log=True)
        set_dag_is_paused(is_paused=True)

        result = get_new_dag_runs(0, [])
        self.validate_result(result, 1, 1, True)

    def test_10_running_paused(self):
        insert_dag_runs(dag_runs_count=1, state="running", with_log=True)
        set_dag_is_paused(is_paused=True)

        result = get_new_dag_runs(1, [])
        self.validate_result(result, 0, 1, True)

    def test_11_extra_dag_runs(self):
        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(2, [1, 2])
        self.validate_result(result, 3, 3)

    def test_12_dag_ids(self):
        insert_dag_runs(dag_runs_count=2, with_log=True)
        insert_dag_runs(dag_id="plugin_other_dag", dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(0, [], ["plugin_other_dag"])

        self.validate_result(result, 3, 5)

    def test_13_exclude_dag_ids(self):
        insert_dag_runs(dag_runs_count=2, with_log=True)
        insert_dag_runs(dag_id="plugin_other_dag", dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(0, [], excluded_dag_ids=["plugin_other_dag"])

        self.validate_result(result, 2, 5)
