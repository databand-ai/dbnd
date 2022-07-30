# © Copyright Databand.ai, an IBM Company 2022


class TestFetchFullRuns(object):
    def validate_result(
        self, result, number_of_dags, number_of_dag_runs, number_of_task_instances
    ):
        assert result

        if number_of_dags > 0:
            assert result.dags
            assert len(result.dags) == number_of_dags

        if number_of_dag_runs > 0:
            assert result.dag_runs
            assert len(result.dag_runs) == number_of_dag_runs

        if number_of_task_instances > 0:
            assert result.task_instances
            assert len(result.task_instances) == number_of_task_instances

    def test_01_empty_db(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs

        dag_run_ids = [1, 2, 3]
        from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader

        dbnd_dag_loader = DbndDagLoader()
        dbnd_dag_loader.load_dags_for_runs(dag_run_ids)

        result = get_full_dag_runs(dag_run_ids, True, dag_loader=dbnd_dag_loader)
        self.validate_result(result, 0, 0, 0)

    def test_02_get_task_instances(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        dag_run_ids = [1, 2, 3]
        from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader

        dbnd_dag_loader = DbndDagLoader()
        dbnd_dag_loader.load_dags_for_runs(dag_run_ids)

        result = get_full_dag_runs(dag_run_ids, True, dbnd_dag_loader)
        self.validate_result(result, 1, 3, 9)

    def test_03_sync_no_sources(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        dag_run_ids = [1, 2, 3]
        from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader

        dbnd_dag_loader = DbndDagLoader()
        dbnd_dag_loader.load_dags_for_runs(dag_run_ids)

        result = get_full_dag_runs(dag_run_ids, False, dag_loader=dbnd_dag_loader)
        self.validate_result(result, 1, 3, 9)
        for dag in result.dags:
            assert not dag.source_code
            for task in dag.tasks:
                assert not task.task_source_code
                assert not task.task_module_code
