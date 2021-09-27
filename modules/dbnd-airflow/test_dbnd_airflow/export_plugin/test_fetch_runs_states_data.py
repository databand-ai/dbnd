class TestFetchDagRunState(object):
    def validate_result(self, result, number_of_task_instances):
        assert result
        if number_of_task_instances > 0:
            assert result.task_instances
            assert len(result.task_instances) == number_of_task_instances

    def test_01_empty_db(self, airflow_dagbag):
        from dbnd_airflow.export_plugin.api_functions import get_dag_runs_states_data

        result = get_dag_runs_states_data([1, 2, 3])
        self.validate_result(result, 0)

    def test_02_get_task_instances(self):
        from dbnd_airflow.export_plugin.api_functions import get_dag_runs_states_data
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        result = get_dag_runs_states_data([1, 2, 3])
        self.validate_result(result, 9)
