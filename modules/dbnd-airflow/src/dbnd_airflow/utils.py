# © Copyright Databand.ai, an IBM Company 2022

import logging
import uuid

from dbnd._core.utils.basics.memoized import cached


logger = logging.getLogger(__name__)


@cached()
def get_airflow_instance_uid():
    """used to distinguish between jobs of different airflow instances"""
    import airflow

    db_url = airflow.settings.Session.bind.engine.url
    db_str = "{}:{}/{}".format(db_url.host, db_url.port, db_url.database)
    airflow_instance_uid = uuid.uuid5(uuid.NAMESPACE_URL, db_str)
    return str(airflow_instance_uid)
