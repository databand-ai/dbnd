# © Copyright Databand.ai, an IBM Company 2022

import logging
import uuid

from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.project.project_fs import abs_join, relative_path
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_attempt_uid


logger = logging.getLogger(__name__)
_airflow_lib_home_default = relative_path(__file__)


def dbnd_airflow_path(*path):
    return abs_join(_airflow_lib_home_default, *path)


def create_airflow_pool(pool_name):
    from airflow.models import Pool
    from airflow.utils.db import create_session

    print("Creating Airflow pool '%s'" % pool_name)
    with create_session() as session:
        if session.query(Pool.pool).filter(Pool.pool == pool_name).scalar() is not None:
            return

        # -1 so we have endless pool
        dbnd_pool = Pool(pool=pool_name, slots=-1)
        session.merge(dbnd_pool)
        session.commit()


def get_task_run_attempt_uid_from_af_ti(ti):
    airflow_instance_uid = get_airflow_instance_uid()
    run_uid = get_job_run_uid(
        airflow_instance_uid=airflow_instance_uid,
        dag_id=ti.dag_id,
        execution_date=ti.execution_date,
    )
    return get_task_run_attempt_uid(run_uid, ti.dag_id, ti.task_id, ti.try_number)


@cached()
def get_airflow_instance_uid():
    """used to distinguish between jobs of different airflow instances"""
    import airflow

    db_url = airflow.settings.Session.bind.engine.url
    db_str = "{}:{}/{}".format(db_url.host, db_url.port, db_url.database)
    airflow_instance_uid = uuid.uuid5(uuid.NAMESPACE_URL, db_str)
    return str(airflow_instance_uid)
