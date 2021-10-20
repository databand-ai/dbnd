class SmartDagBag:
    def __init__(self):
        self._existing_file_paths = set()

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

    def _get_dagbag(self, dags_file_paths, dag_bag=None):
        if not dag_bag:
            from airflow import models

            from tempfile import mkdtemp

            tmp_dir = mkdtemp()
            dag_bag = models.DagBag(tmp_dir, include_examples=False)

        for file_path in dags_file_paths:
            if file_path not in self._existing_file_paths:
                dag_bag.collect_dags(file_path, include_examples=False)

        return dag_bag

    def get_dagbag(self, dag_run_ids, existing_dagbag, session):
        dag_ids = self._get_dag_ids(dag_run_ids, session)
        file_paths = self._get_dag_files_paths(dag_ids, session)
        dagbag = self._get_dagbag(file_paths, existing_dagbag)
        self._existing_file_paths.update(file_paths)
        return dagbag
