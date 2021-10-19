from airflow.utils.db import provide_session


class TestFetchFullRuns(object):
    def _get_dagbag(self, dags_file_paths, existing_file_paths, dag_bag=None):
        if not dag_bag:
            from airflow.configuration import conf

            self._safe_mode = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE")

            from tempfile import mkdtemp

            tmp_dir = mkdtemp()

            from airflow import models, settings

            if hasattr(settings, "STORE_SERIALIZED_DAGS"):
                from airflow.settings import STORE_SERIALIZED_DAGS

                dag_bag = models.DagBag(
                    tmp_dir,
                    include_examples=False,
                    store_serialized_dags=STORE_SERIALIZED_DAGS,
                )
            else:
                dag_bag = models.DagBag(tmp_dir, include_examples=False)

        for file_path in dags_file_paths:
            if file_path not in existing_file_paths:
                dag_bag.collect_dags(
                    file_path, include_examples=False, safe_mode=self._safe_mode
                )

        existing_file_paths.update(dags_file_paths)

        return dag_bag

    def _get_dag_ids(self, dag_run_ids, session):
        from airflow.models import DagRun

        result = session.query(DagRun.dag_id).filter(DagRun.id.in_(dag_run_ids)).all()
        dag_ids = set([r[0] for r in result])

        return dag_ids

    def _get_dag_files_paths(self, dag_ids, session):
        from airflow.models import DagModel

        result = (
            session.query(DagModel.fileloc).filter(DagModel.dag_id.in_(dag_ids)).all()
        )
        paths = set([r[0] for r in result])

        return paths

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
    def get_dag_bag(self, dag_run_ids, existing_file_paths, dag_bag, session):
        dag_ids = self._get_dag_ids(dag_run_ids, session)
        dag_files_paths = self._get_dag_files_paths(dag_ids, session)
        dagbag = self._get_dagbag(dag_files_paths, existing_file_paths, dag_bag)

        return dagbag

    def test_01_empty_db(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs

        dag_run_ids = [1, 2, 3]
        existing_file_paths = set()
        airflow_dagbag = self.get_dag_bag(dag_run_ids, existing_file_paths)

        result = get_full_dag_runs(dag_run_ids, True, airflow_dagbag)
        self.validate_result(result, 0, 0, 0)

    def test_02_get_task_instances(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        dag_run_ids = [1, 2, 3]
        existing_file_paths = set()
        airflow_dagbag = self.get_dag_bag(dag_run_ids, existing_file_paths)
        result = get_full_dag_runs(dag_run_ids, True, airflow_dagbag)
        self.validate_result(result, 1, 3, 9)

    def test_03_sync_no_sources(self):
        from dbnd_airflow.export_plugin.api_functions import get_full_dag_runs
        from test_dbnd_airflow.export_plugin.db_data_generator import insert_dag_runs

        insert_dag_runs(dag_runs_count=3, task_instances_per_run=3)

        dag_run_ids = [1, 2, 3]
        existing_file_paths = set()
        airflow_dagbag = self.get_dag_bag(dag_run_ids, existing_file_paths)
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

        existing_file_paths = set()
        airflow_dagbag = None

        airflow_dagbag = self.get_dag_bag([1, 2], existing_file_paths, airflow_dagbag)
        assert set(airflow_dagbag.dag_ids) == {"plugin_test_dag_1", "plugin_test_dag_2"}

        airflow_dagbag = self.get_dag_bag([3, 4], existing_file_paths, airflow_dagbag)
        assert set(airflow_dagbag.dag_ids) == {
            "plugin_test_dag_1",
            "plugin_test_dag_2",
            "plugin_test_dag_3",
            "plugin_test_dag_4",
        }

        airflow_dagbag = self.get_dag_bag([5, 6], existing_file_paths, airflow_dagbag)
        assert set(airflow_dagbag.dag_ids) == {
            "plugin_test_dag_1",
            "plugin_test_dag_2",
            "plugin_test_dag_3",
            "plugin_test_dag_4",
            "plugin_test_dag_5",
            "plugin_test_dag_6",
        }
