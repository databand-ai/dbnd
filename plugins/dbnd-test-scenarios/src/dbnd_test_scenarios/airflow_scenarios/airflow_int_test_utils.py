import logging
import os

import requests

from tenacity import retry, stop_after_attempt, wait_fixed


logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(5), wait=wait_fixed(5), reraise=True)
def assert_airflow_dag_exists_on_webserver(dag_id):
    # this will fail if we got a redirect - which mean that airflow couldn't find the dag
    # is may not be necessary true, so we are retrying multiple times
    AIRFLOW__WEBSERVER__URL = os.environ.get("AIRFLOW__WEBSERVER__URL")
    target = AIRFLOW__WEBSERVER__URL + "/admin/airflow/tree?dag_id=%s" % dag_id
    logger.info("Checking dag on Airflow server at %s", target)
    response = requests.get(target)
    # making sure we didn't get redirected
    assert response.history == [], "%s wasn't found on webserver" % dag_id
