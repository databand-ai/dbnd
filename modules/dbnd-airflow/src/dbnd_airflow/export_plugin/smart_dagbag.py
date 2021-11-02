import logging

from collections import defaultdict

from airflow.utils.db import provide_session


logger = logging.getLogger(__name__)


class DbndDagLoader(object):
    def __init__(self):
        self._dags = {}
        self._file_to_dags = defaultdict(list)

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

    def _load_from_file(self, file_path):
        try:
            from airflow import models

            dag_bag = models.DagBag(file_path, include_examples=False)
            return dag_bag.dags
        except Exception:
            logger.info("Failed to load dag from %s", file_path)

    def load_dags_from_files(self, dags_file_paths):
        for file_path in dags_file_paths:
            if file_path not in self._file_to_dags:
                dags = self._load_from_file(file_path)
                self._file_to_dags[file_path] = dags.values()
                self._dags.update(dags)

    def load_dags(self, dag_ids, session):
        file_paths = self._get_dag_files_paths(dag_ids, session)
        self.load_dags_from_files(file_paths)

    @provide_session
    def load_dags_for_runs(self, dag_run_ids, session):
        dag_ids = self._get_dag_ids(dag_run_ids=dag_run_ids, session=session)
        self.load_dags(dag_ids=dag_ids, session=session)

    def load_from_dag_bag(self, dag_bag):
        self._dags.update(dag_bag.dags)
        for dag_id, dag in dag_bag.dags.items():
            self._file_to_dags[dag.fileloc].append(dag)

    def get_dags(self):
        return self._dags

    def get_dag(self, dag_id):
        return self._dags.get(dag_id)
