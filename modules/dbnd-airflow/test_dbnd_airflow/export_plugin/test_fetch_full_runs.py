from airflow.utils.db import provide_session


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

    @provide_session
    def get_dagbag(self, dag_run_ids, smart_dagbag, airflow_dagbag, session):
        return smart_dagbag.get_dagbag(dag_run_ids, airflow_dagbag, session)

    def test_01_empty_db(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs

        dag_run_ids = [1, 2, 3]
        airflow_dagbag = None
        from dbnd_airflow.export_plugin.smart_dagbag import SmartDagBag

        smart_dagbag = SmartDagBag()
        airflow_dagbag = self.get_dagbag(dag_run_ids, smart_dagbag, airflow_dagbag)

        result = get_full_dag_runs(dag_run_ids, True, airflow_dagbag)
        self.validate_result(result, 0, 0, 0)

    def test_02_get_task_instances(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        dag_run_ids = [1, 2, 3]
        airflow_dagbag = None
        from dbnd_airflow.export_plugin.smart_dagbag import SmartDagBag

        smart_dagbag = SmartDagBag()
        airflow_dagbag = self.get_dagbag(dag_run_ids, smart_dagbag, airflow_dagbag)

        result = get_full_dag_runs(dag_run_ids, True, airflow_dagbag)
        self.validate_result(result, 1, 3, 9)

    def test_03_sync_no_sources(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        dag_run_ids = [1, 2, 3]
        airflow_dagbag = None
        from dbnd_airflow.export_plugin.smart_dagbag import SmartDagBag

        smart_dagbag = SmartDagBag()
        airflow_dagbag = self.get_dagbag(dag_run_ids, smart_dagbag, airflow_dagbag)

        result = get_full_dag_runs(dag_run_ids, False, airflow_dagbag)
        self.validate_result(result, 1, 3, 9)
        for dag in result.dags:
            assert not dag.source_code
            for task in dag.tasks:
                assert not task.task_source_code
                assert not task.task_module_code

    def test_dag_bag(self):
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(
            dag_runs_count=1, task_instances_per_run=3, dag_id="plugin_test_dag_1"
        )
        insert_dag_runs(
            dag_runs_count=1, task_instances_per_run=3, dag_id="plugin_test_dag_2"
        )
        insert_dag_runs(
            dag_runs_count=1, task_instances_per_run=3, dag_id="plugin_test_dag_3"
        )
        insert_dag_runs(
            dag_runs_count=1, task_instances_per_run=3, dag_id="plugin_test_dag_4"
        )
        insert_dag_runs(
            dag_runs_count=1, task_instances_per_run=3, dag_id="plugin_test_dag_5"
        )
        insert_dag_runs(
            dag_runs_count=1, task_instances_per_run=3, dag_id="plugin_test_dag_6"
        )

        airflow_dagbag = None
        from dbnd_airflow.export_plugin.smart_dagbag import SmartDagBag

        smart_dagbag = SmartDagBag()

        airflow_dagbag = self.get_dagbag([1, 2], smart_dagbag, airflow_dagbag)
        assert set(airflow_dagbag.dag_ids) == {"plugin_test_dag_1", "plugin_test_dag_2"}

        airflow_dagbag = self.get_dagbag([3, 4], smart_dagbag, airflow_dagbag)
        assert set(airflow_dagbag.dag_ids) == {
            "plugin_test_dag_1",
            "plugin_test_dag_2",
            "plugin_test_dag_3",
            "plugin_test_dag_4",
        }

        airflow_dagbag = self.get_dagbag([5, 6], smart_dagbag, airflow_dagbag)
        assert set(airflow_dagbag.dag_ids) == {
            "plugin_test_dag_1",
            "plugin_test_dag_2",
            "plugin_test_dag_3",
            "plugin_test_dag_4",
            "plugin_test_dag_5",
            "plugin_test_dag_6",
        }
