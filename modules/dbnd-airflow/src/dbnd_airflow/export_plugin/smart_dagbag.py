import logging

from collections import defaultdict

from airflow.utils.db import provide_session


logger = logging.getLogger(__name__)


class DbndDagLoader(object):
    def __init__(self):
        # Mapping between dag_id to dag object
        self._dags = {}

        # Mapping between file path to a list of dag objects
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

            # Use DagBag module to load all dags from a given file
            dag_bag = models.DagBag(file_path, include_examples=False)

            # Now the DagBag object contains the 'dags' dict which maps between each dag id to the dag object
            return dag_bag.dags
        except Exception:
            logger.warning(
                "Failed to load dag from %s. Exception:", file_path, exc_info=True
            )
        except SystemExit:
            logger.warning(
                "Failed to load dag from %s, due to SystemExit",
                file_path,
                exc_info=True,
            )

        return None

    def load_dags_from_files(self, dags_file_paths):
        for file_path in dags_file_paths:
            if file_path not in self._file_to_dags:
                dags = self._load_from_file(file_path)
                if dags:
                    self._file_to_dags[file_path] = dags.values()
                    self._dags.update(dags)
                else:
                    self._file_to_dags[file_path] = []

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
        return self._dags.get(dag_id, None)
