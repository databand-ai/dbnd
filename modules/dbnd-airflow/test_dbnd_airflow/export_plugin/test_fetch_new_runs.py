import mock


class TestNewRuns(object):
    def validate_result(
        self,
        result,
        expected_dag_runs,
        expected_max_dag_run_id,
        expected_max_log_id,
        is_paused=False,
        expected_max_log_ids=None,
    ):
        assert result
        assert result.error_message is None
        assert len(result.new_dag_runs) == expected_dag_runs
        assert result.last_seen_dag_run_id is expected_max_dag_run_id
        assert result.last_seen_log_id is expected_max_log_id

        result.new_dag_runs = sorted(result.new_dag_runs, key=lambda r: r.id)

        if result.new_dag_runs:
            paused_runs = [
                new_run for new_run in result.new_dag_runs if new_run.is_paused
            ]
            if is_paused:
                assert len(paused_runs) == expected_dag_runs
            else:
                assert len(paused_runs) == 0

            if not expected_max_log_ids:
                new_run_max_log_ids = [
                    new_run.max_log_id
                    for new_run in result.new_dag_runs
                    if new_run.max_log_id is not None
                ]
                assert len(new_run_max_log_ids) == 0
                runs_with_updated_task_instances = [
                    new_run.has_updated_task_instances
                    for new_run in result.new_dag_runs
                    if new_run.has_updated_task_instances
                ]
                assert len(runs_with_updated_task_instances) == 0
            else:
                for i, new_dag_run in enumerate(result.new_dag_runs):
                    assert new_dag_run.max_log_id == expected_max_log_ids[i]
                    if expected_max_log_ids[i] is not None:
                        assert new_dag_run.has_updated_task_instances is True
                    else:
                        assert new_dag_run.has_updated_task_instances is False

                    if new_dag_run.has_updated_task_instances:
                        assert new_dag_run.events == ["success"]

    def test_01_empty_db(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs

        result = get_new_dag_runs(1, 1, [])
        self.validate_result(result, 0, None, None)

    def test_02_both_none(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(None, None, [])
        self.validate_result(result, 0, 3, 3)

    def test_03_running(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, state="running", with_log=False)

        result = get_new_dag_runs(None, None, [])
        self.validate_result(result, 3, 3, None)

    def test_04_dag_run_id_none(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(None, 1, [])
        self.validate_result(result, 2, 3, 3, expected_max_log_ids=[2, 3])

    def test_05_log_id_none(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(1, None, [])
        self.validate_result(result, 2, 3, 3)

    def test_06_both_0(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(with_log=True)

        result = get_new_dag_runs(0, 0, [])
        self.validate_result(result, 1, 1, 1, expected_max_log_ids=[1])

    def test_07_big_run_id(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=False)

        result = get_new_dag_runs(3, 0, [])
        self.validate_result(result, 0, 3, None)

        assert result

    def test_08_big_log_id(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=False)

        result = get_new_dag_runs(0, 3, [])
        self.validate_result(result, 3, 3, None)

    def test_09_paused(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import (
            insert_dag_runs,
            set_dag_is_paused,
        )

        insert_dag_runs(dag_runs_count=1, with_log=True)
        set_dag_is_paused(is_paused=True)
        result = get_new_dag_runs(0, 0, [])
        self.validate_result(result, 1, 1, 1, True, expected_max_log_ids=[1])

    def test_10_running_paused(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import (
            insert_dag_runs,
            set_dag_is_paused,
        )

        insert_dag_runs(dag_runs_count=1, state="running", with_log=True)
        set_dag_is_paused(is_paused=True)
        result = get_new_dag_runs(1, 1, [])
        self.validate_result(result, 0, 1, 1, True)

    def test_11_extra_dag_runs(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(2, 2, [1, 2])
        self.validate_result(result, 3, 3, 3, expected_max_log_ids=[None, None, 3])

    def test_12_dag_ids(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, with_log=True)
        insert_dag_runs(dag_id="plugin_other_dag", dag_runs_count=3, with_log=True)

        result = get_new_dag_runs(0, 0, [], ["plugin_other_dag"])

        self.validate_result(result, 3, 6, 6, expected_max_log_ids=[4, 5, 6])

    def test_13_fetch_in_chunks(self):
        from dbnd_airflow.export_plugin.api_functions import get_new_dag_runs
        from dbnd_airflow.export_plugin.queries import (
            MAX_PARAMETERS_INSIDE_IN_CLAUSE,
            _find_dag_runs_by_list_in_chunks,
        )
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        with mock.patch(
            "dbnd_airflow.export_plugin.queries._find_dag_runs_by_list_in_chunks",
            wraps=_find_dag_runs_by_list_in_chunks,
        ) as m:
            insert_dag_runs(
                dag_runs_count=MAX_PARAMETERS_INSIDE_IN_CLAUSE - 1, with_log=True
            )
            get_new_dag_runs(0, 0, [], [])
            assert m.call_count == 0

            insert_dag_runs(dag_runs_count=1, with_log=True)
            get_new_dag_runs(0, 0, [], [])
            assert m.call_count == 1
