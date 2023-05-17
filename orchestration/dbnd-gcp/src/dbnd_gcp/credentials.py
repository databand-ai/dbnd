# © Copyright Databand.ai, an IBM Company 2022

import logging

from google.auth import default

from dbnd._core.context.use_dbnd_run import use_airflow_connections
from dbnd._core.utils.basics.memoized import per_thread_cached


logger = logging.getLogger(__name__)


@per_thread_cached()
def get_gc_credentials():
    if use_airflow_connections():
        from dbnd_run.airflow.dbnd_airflow_contrib.credentials_helper_gcp import (
            GSCredentials,
        )

        gcp_credentials = GSCredentials()
        logger.debug(
            "getting gcp credentials from airflow connection '%s'"
            % gcp_credentials.gcp_conn_id
        )
        return gcp_credentials.get_credentials()
    else:
        logger.debug(
            "getting gcp credentials from environment using google.auth.default()"
        )
        credentials, _ = default()
        return credentials
