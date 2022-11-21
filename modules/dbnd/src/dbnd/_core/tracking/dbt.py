# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import os.path

from enum import Enum
from typing import Dict, T

import yaml

from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.tracking.metrics import TRACKER_MISSING_MESSAGE, _get_tracker
from dbnd._core.utils.one_time_logger import get_one_time_logger
from dbnd.utils.dbt_cloud_api_client import DbtCloudApiClient


logger = logging.getLogger(__name__)


def _report_dbt_metadata(dbt_metadata, tracker=None):
    if tracker is None:
        tracker = _get_tracker()

    if not tracker:
        message = TRACKER_MISSING_MESSAGE % ("report_dbt_metadata",)
        get_one_time_logger().log_once(message, "report_dbt_metadata", logging.WARNING)
        return

    tracker.log_dbt_metadata(dbt_metadata=dbt_metadata)


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


def collect_data_from_dbt_cloud(
    dbt_cloud_account_id, dbt_cloud_api_token, dbt_job_run_id
):
    """
    Collect metadata for a single run from dbt cloud.

    Args:
        dbt_cloud_account_id: dbt cloud account id in order to  identify with dbt cloud API
        dbt_cloud_api_token: Api token in order to authenticate dbt cloud API
        dbt_job_run_id: run id of the dbt run that we want to report it's metadata.

        @task
        def prepare_data():
            collect_data_from_dbt_cloud(
            dbt_cloud_account_id=my_dbt_cloud_account_id,
            dbt_cloud_api_token="my_dbt_cloud_api_token",
            dbt_job_run_id=12345
            )
    """
    if not dbt_job_run_id:
        logger.warning("Can't collect run  Data from dbt cloud,missing run id")
        return

    if not dbt_cloud_api_token or not dbt_cloud_account_id:
        logger.warning(
            "Can't collect Data from dbt cloud, account id nor api key were supplied"
        )
        return

    try:
        dbt_cloud_client = DbtCloudApiClient(
            account_id=dbt_cloud_account_id, dbt_cloud_api_token=dbt_cloud_api_token
        )

        dbt_run_meta_data = get_run_data_from_dbt(dbt_cloud_client, dbt_job_run_id)
        if not dbt_run_meta_data:
            return

        _report_dbt_metadata(dbt_run_meta_data)
    except Exception as e:
        logger.warning(
            "Failed collect and report data from dbt cloud api,continue execution"
        )
        log_exception("Could not collect data from dbt cloud", e)


def collect_data_from_dbt_core(dbt_project_path: str):
    # open runs_info.json file and read it as a dict
    with open(
        os.path.join(dbt_project_path, "target", "run_results.json"), "r"
    ) as runs_info_file:
        runs_info = json.loads(runs_info_file.read())
    # open manifest.json file and read it as a dict
    with open(
        os.path.join(dbt_project_path, "target", "manifest.json"), "r"
    ) as manifest_file:
        manifest = json.loads(manifest_file.read())

    run_total_duration = runs_info["elapsed_time"]

    status = "pass"
    n = next((x for x in runs_info["results"] if x["status"] != "pass"), None)
    if n is None:
        status = "fail"

    status_humanized = status

    with open(os.path.join(dbt_project_path, "logs", "dbt.log"), "r") as logs_file:
        logs = logs_file.read()

    dbt_step_meta_data = {
        "status_humanized": status_humanized,
        "run_results": runs_info,
        "manifest": manifest,
        "duration": run_total_duration,
        "index": 1,
        "created_at": runs_info["metadata"]["generated_at"],
        "started_at": calculate_started_time(runs_info),
        "logs": logs,
        "finished_at": calculate_finished_time(runs_info),
        "name": f"dbt {runs_info['args']['which']}",
    }

    project = load_yaml_with_jinja(os.path.join(dbt_project_path, "dbt_project.yml"))
    profile_dir = runs_info["args"]["profiles_dir"]

    profile_name = project["profile"]

    profile = load_yaml_with_jinja(os.path.join(profile_dir, "profiles.yml"))[
        profile_name
    ]

    target = profile["target"]
    if "target" in runs_info["args"]:
        target = runs_info["args"]["target"]

    profile = profile["outputs"][target]

    adapter = extract_adapter_type(profile)
    namespace = extract_dataset_namespace(adapter, profile)

    _report_dbt_metadata(
        {
            "status_humanized": status_humanized,
            "environment": {
                "connection": {"type": adapter.value, "hostname": namespace}
            },
            "run_steps": [dbt_step_meta_data],
        }
    )


def calculate_started_time(runs_info):
    for run in runs_info["results"]:
        if "timing" in run and "started_at" in run["timing"][0]:
            return runs_info["results"][0]["timing"][0]["started_at"]

    return None


def calculate_finished_time(runs_info):
    for run in runs_info["results"][::-1]:
        if "timing" in run and "completed_at" in run["timing"][-1]:
            return runs_info["results"][-1]["timing"][-1]["completed_at"]

    return None


def load_yaml(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def load_yaml_with_jinja(path: str) -> Dict:
    loaded = load_yaml(path)
    return render_values_jinja(value=loaded)


def render_values_jinja(value: T) -> T:
    """
    Traverses passed dictionary and render any string value using jinja.
    Returns copy of the dict with parsed values.
    """
    if isinstance(value, dict):
        parsed_dict = {}
        for key, val in value.items():
            parsed_dict[key] = render_values_jinja(val)
        return parsed_dict  # type: ignore
    elif isinstance(value, list):
        parsed_list = []
        for elem in value:
            parsed_list.append(render_values_jinja(elem))
        return parsed_list  # type: ignore
    else:
        return value


class Adapter(Enum):
    # This class represents supported adapters.
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    SPARK = "spark"

    @staticmethod
    def adapters() -> str:
        # String representation of all supported adapter names
        return ",".join([f"`{x.value}`" for x in list(Adapter)])


class SparkConnectionMethod(Enum):
    THRIFT = "thrift"
    ODBC = "odbc"
    HTTP = "http"

    @staticmethod
    def methods():
        return [x.value for x in SparkConnectionMethod]


def extract_adapter_type(profile: Dict):
    try:
        return Adapter[profile["type"].upper()]
    except KeyError:
        raise NotImplementedError(
            f"Only {Adapter.adapters()} adapters are supported right now. "
            f"Passed {profile['type']}"
        )


def extract_dataset_namespace(adapter_type, profile: Dict):
    return extract_namespace(adapter_type, profile)


def extract_namespace(adapter_type, profile: Dict) -> str:
    """Extract namespace from profile's type"""
    if adapter_type == Adapter.SNOWFLAKE:
        return f"snowflake://{profile['account']}"
    elif adapter_type == Adapter.BIGQUERY:
        return "bigquery"
    elif adapter_type == Adapter.REDSHIFT:
        return f"redshift://{profile['host']}:{profile['port']}"
    elif adapter_type == Adapter.SPARK:
        port = ""

        if "port" in profile:
            port = f":{profile['port']}"
        elif profile["method"] in [
            SparkConnectionMethod.HTTP.value,
            SparkConnectionMethod.ODBC.value,
        ]:
            port = "443"
        elif profile["method"] == SparkConnectionMethod.THRIFT.value:
            port = "10001"

        if profile["method"] in SparkConnectionMethod.methods():
            return f"spark://{profile['host']}{port}"
        else:
            raise NotImplementedError(
                f"Connection method `{profile['method']}` is not "
                f"supported for spark adapter."
            )
    else:
        raise NotImplementedError(
            f"Only {Adapter.adapters()} adapters are supported right now. "
            f"Passed {profile['type']}"
        )
