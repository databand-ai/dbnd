import logging
import os

from airflow.models import Variable

from dbnd._core.utils.basics.helpers import parse_bool
from dbnd_airflow.constants import AIRFLOW_VERSION_2


if AIRFLOW_VERSION_2:
    from airflow.hooks.base import BaseHook
else:
    from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)
ENV_AIRFLOW_HOME = os.environ.get("ENV_AIRFLOW_HOME")
if not ENV_AIRFLOW_HOME:
    raise Exception("Please `source set-env.sh` before you run this code ")

# From set-env.sh or Docker env vars (injected on build
DIST_HOME = os.environ.get("DIST_HOME")
DBND_VERSION = os.environ.get("DBND_VERSION")
DBND_SOURCE_COMMIT = os.environ.get("DBND_SOURCE_COMMIT")

ENV_AIRFLOW_NAME_SLUG = Variable.get("ENV_AIRFLOW_NAME_SLUG", None)
if not ENV_AIRFLOW_NAME_SLUG:
    raise Exception(
        "Please make sure you have ran `set-vars-connections.sh` before you run this code "
    )

ENV_AIRFLOW__INSIDE_DOCKER_COMPOSE = parse_bool(
    os.environ.get("ENV_AIRFLOW__INSIDE_DOCKER_COMPOSE", "false")
)


# DBND_STAGING_LOCAL_DATA_LAKE = DbndStagingDataLake( f"{ENV_AIRFLOW_HOME}/data" )


def get_databand_url_and_token():
    dbnd_config = BaseHook.get_connection("dbnd_config").extra_dejson
    return (
        dbnd_config["core"]["databand_url"],
        dbnd_config["core"]["databand_access_token"],
    )


def get_op_output(root_dir, output_format="csv"):
    return os.path.join(
        root_dir,
        "{{dag.dag_id}}",
        "{{task.task_id}}",
        "output_{{ts_nodash}}.%s" % output_format,
    )


class DbndStagingDataLake(object):
    def __init__(self, root):
        self.data_root = os.path.join(root, "data")

    def get_op_output(self, output_format="csv"):
        return get_op_output(f"{self.data_root}/output", output_format=output_format)

    def get_input_311_daily_data(self):
        """
        Very specific input, copied during deployment
        """
        return os.path.join(
            self.data_root, "daily_data", "date=2022-02-15", "daily_data.csv"
        )


STAGING_AWS_ROOT = Variable.get("STAGING_AWS_ROOT")
STAGING_AWS = DbndStagingDataLake(STAGING_AWS_ROOT)

STAGING_GCP_ROOT = Variable.get("STAGING_GCP_ROOT")
STAGING_GCP = DbndStagingDataLake(STAGING_GCP_ROOT)

STAGING_WORKING_DIR_LOCAL = "/tmp"
