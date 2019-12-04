from __future__ import absolute_import

import logging

import pendulum

from airflow.models import DagBag, DagPickle
from airflow.utils.db import provide_session

from dbnd_airflow.airflow_override import DbndAirflowTaskInstance


logger = logging.getLogger(__name__)


DAG_UNPICKABLE_PROPERTIES = (
    "_log",
    ("user_defined_macros", {}),
    ("user_defined_filters", {}),
    ("params", {}),
)


class DbndAirflowDagBag(DagBag):
    @provide_session
    def get_dag(self, dag_id, execution_date=None, session=None):
        """
        :param dag_id:
        :param execution_date: if provided, we'll try to find specifc version of dag (using pickle)
        :param session:
        :return:
        """
        from flask import has_request_context, request, session as flask_session

        # all legacy airflow code works just with dag_id, also, there are some calls that doesn't pass through execution_date
        if has_request_context():
            execution_date = execution_date or request.args.get("execution_date")

            # trick to store last execution date used for the next flask call
            if execution_date:
                logger.debug(
                    "Execution date saved to session: %s, %s", dag_id, execution_date
                )
                flask_session["ed_" + dag_id] = execution_date
            else:
                logger.debug("Execution date from previous session: %s", dag_id)
                execution_date = flask_session.get("ed_" + dag_id)

            if execution_date and execution_date != "undefined":
                # we are going to return most "active" dag
                dttm = pendulum.parse(execution_date)
                dag = self._get_pickled_dag_from_dagrun(
                    dag_id=dag_id, execution_date=dttm, session=session
                )
                if dag:
                    return dag

        # we don't have specific dag/execution date, we are trying to get in-memory version
        dag = super(DbndAirflowDagBag, self).get_dag(dag_id)
        if dag:
            return dag

        # let try to find it latest version in DB
        latest_execution = (
            session.query(DbndAirflowTaskInstance.execution_date)
            .filter(DbndAirflowTaskInstance.task_id == dag_id)
            .order_by(DbndAirflowTaskInstance.execution_date.desc())
            .first()
        )

        if latest_execution:
            return self._get_pickled_dag_from_dagrun(
                dag_id=dag_id,
                execution_date=latest_execution.execution_date,
                session=session,
            )

        return None

    @provide_session
    def _get_pickled_dag_from_dagrun(self, dag_id, execution_date, session=None):

        ti = (
            session.query(DbndAirflowTaskInstance.executor_config)
            .filter(
                DbndAirflowTaskInstance.dag_id == dag_id,
                DbndAirflowTaskInstance.execution_date == execution_date,
            )
            .first()
        )
        if not ti:
            return None

        pickled_dag_id = ti.executor_config.get("DatabandExecutor", {}).get(
            "dag_pickle_id", None
        )
        try:
            pickled_dag = (
                session.query(DagPickle)
                .filter(DagPickle.id == pickled_dag_id)
                .one_or_none()
            )
        except Exception as ex:
            logger.error("Error ocured during DAG retrieval from DB, %s", ex)
            return None

        if pickled_dag and pickled_dag.pickle:
            # we found pickled dag
            dag = pickled_dag.pickle
            dag.dag_version_execution_date = execution_date
            # let's add this dag into dags, there is a check that validates if this dag exists..
            self.dags[dag_id] = dag
            return dag
        else:
            # failed to parse dag?
            return None
