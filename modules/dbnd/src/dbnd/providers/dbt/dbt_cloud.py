# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.tracking.metrics import TRACKER_MISSING_MESSAGE, _get_tracker
from dbnd._core.utils.one_time_logger import get_one_time_logger
from dbnd.providers.dbt.dbt_cloud_api_client import DbtCloudApiClient


logger = logging.getLogger(__name__)

DBT_CLOUD_SOURCE = "dbt_cloud"
DEFAULT_API_URL = "https://cloud.getdbt.com"


def collect_data_from_dbt_cloud(
    dbt_cloud_account_id,
    dbt_cloud_api_token,
    dbt_job_run_id,
    dbt_api_url=DEFAULT_API_URL,
):
    """
    Collect metadata for a single run from dbt cloud.

    Args:
        dbt_cloud_account_id: dbt cloud account id in order to  identify with dbt cloud API
        dbt_cloud_api_token: Api token in order to authenticate dbt cloud API
        dbt_job_run_id: run id of the dbt run that we want to report it's metadata.
        dbt_api_url: user's dbt URL

        @task
        def prepare_data():
            collect_data_from_dbt_cloud(
            dbt_cloud_account_id=my_dbt_cloud_account_id,
            dbt_cloud_api_token="my_dbt_cloud_api_token",
            dbt_job_run_id=12345
            dbt_api_url="https://ab123.us1.dbt.com
            )
    """
    if not dbt_job_run_id:
        logger.warning("Can't collect run  Data from dbt cloud, missing run id")
        return

    if not dbt_cloud_api_token or not dbt_cloud_account_id:
        logger.warning(
            "Can't collect Data from dbt cloud, account id nor api key were supplied"
        )
        return

    tracker = _get_tracker()

    if not tracker:
        message = TRACKER_MISSING_MESSAGE % ("collect_data_from_dbt_cloud",)
        get_one_time_logger().log_once(
            message, "collect_data_from_dbt_cloud", logging.WARNING
        )
        return

    try:
        dbt_cloud_client = DbtCloudApiClient(
            account_id=dbt_cloud_account_id,
            dbt_cloud_api_token=dbt_cloud_api_token,
            dbt_api_url=dbt_api_url,
        )

        dbt_run_meta_data = get_run_data_from_dbt(dbt_cloud_client, dbt_job_run_id)
        if not dbt_run_meta_data:
            return
        dbt_run_meta_data["reported_from"] = DBT_CLOUD_SOURCE
        tracker.log_dbt_metadata(dbt_metadata=dbt_run_meta_data)
    except Exception as e:
        logger.warning(
            "Failed collect and report data from dbt cloud api,continue execution"
        )
        log_exception("Could not collect data from dbt cloud", e)


def get_run_data_from_dbt(dbt_cloud_client, dbt_job_run_id):
    dbt_run_meta_data = dbt_cloud_client.get_run(run_id=dbt_job_run_id)

    if not dbt_run_meta_data:
        logger.warning("Fail getting run data from dbt cloud ")
        return None

    env_id = dbt_run_meta_data.get("environment_id")
    env = dbt_cloud_client.get_environment(env_id=env_id)

    if env:
        dbt_run_meta_data["environment"] = env

    for step in dbt_run_meta_data.get("run_steps", []):
        step_run_results_artifact = dbt_cloud_client.get_run_results_artifact(
            run_id=dbt_job_run_id, step=step["index"]
        )
        if step_run_results_artifact:
            step["run_results"] = step_run_results_artifact

        step_run_manifest_artifact = dbt_cloud_client.get_manifest_artifact(
            run_id=dbt_job_run_id, step=step["index"]
        )

        if step_run_manifest_artifact:
            step["manifest"] = step_run_manifest_artifact

    return dbt_run_meta_data
