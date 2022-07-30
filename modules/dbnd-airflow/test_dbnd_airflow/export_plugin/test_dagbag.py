# © Copyright Databand.ai, an IBM Company 2022

import os


class TestDagbag(object):
    def test_dag_bag(self):
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        for i in range(1, 7):
            insert_dag_runs(
                dag_runs_count=1,
                task_instances_per_run=3,
                dag_id=f"plugin_test_dag_{i}",
            )

        from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader

        dbnd_dag_loader = DbndDagLoader()
        dbnd_dag_loader.load_dags_for_runs([1, 2])
        actual_ids = {d.dag_id for d in dbnd_dag_loader.get_dags().values()}
        assert actual_ids == {"plugin_test_dag_1", "plugin_test_dag_2"}

        dbnd_dag_loader.load_dags_for_runs([3, 4])
        actual_ids = {d.dag_id for d in dbnd_dag_loader.get_dags().values()}
        assert actual_ids == {
            "plugin_test_dag_1",
            "plugin_test_dag_2",
            "plugin_test_dag_3",
            "plugin_test_dag_4",
        }

        dbnd_dag_loader.load_dags_for_runs([5, 6])

        actual_ids = {d.dag_id for d in dbnd_dag_loader.get_dags().values()}
        assert actual_ids == {
            "plugin_test_dag_1",
            "plugin_test_dag_2",
            "plugin_test_dag_3",
            "plugin_test_dag_4",
            "plugin_test_dag_5",
            "plugin_test_dag_6",
        }

    def test_dag_with_exit(self, caplog):
        from dbnd_airflow.export_plugin.smart_dagbag import DbndDagLoader

        dbnd_dag_loader = DbndDagLoader()
        dag_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "dag_with_exit.py"
        )
        assert dbnd_dag_loader._load_from_file(dag_path) is None

        assert "SystemExit" in caplog.text
