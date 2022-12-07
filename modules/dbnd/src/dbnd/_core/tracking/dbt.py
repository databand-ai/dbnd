# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import os.path

from enum import Enum
from typing import Dict, Optional, TypeVar

import attr
import yaml

from jinja2 import Environment, TemplateError

from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.tracking.metrics import TRACKER_MISSING_MESSAGE, _get_tracker
from dbnd._core.utils.one_time_logger import get_one_time_logger
from dbnd.utils.dbt_cloud_api_client import DbtCloudApiClient


logger = logging.getLogger(__name__)

T = TypeVar("T")


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


################
#              #
#   DBT CORE   #
#              #
################


def collect_data_from_dbt_core(dbt_project_path: str):
    phase: str = ""
    try:
        phase = "load"
        assets = _load_dbt_core_assets(dbt_project_path)

        phase = "extract-metadata"
        data = assets.extract_metadata()

        phase = "report"
        _report_dbt_metadata(data)
    except Exception as e:
        logger.warning(
            "Failed to %s data from dbt core at path %s,continue execution",
            phase,
            dbt_project_path,
        )
        log_exception(f"Could not {phase} data from dbt cloud", e)


@attr.s(auto_attribs=True)
class DbtCoreAssets:
    runs_info: dict
    manifest: dict
    logs: str
    profile: dict

    def extract_metadata(self):
        status_humanized = _extract_status(self.runs_info)

        environment = _extract_environment(self.runs_info, self.profile)

        dbt_step_meta_data = _extract_step_meta_data(
            self.runs_info, self.manifest, self.logs
        )

        data = {
            "status_humanized": status_humanized,
            "environment": environment,
            "run_steps": [dbt_step_meta_data],
            "id": dbt_step_meta_data["run_results"]["metadata"]["invocation_id"],
            "job": {"name": dbt_step_meta_data["run_results"]["args"]["which"]},
            "job_id": dbt_step_meta_data["run_results"]["metadata"]["invocation_id"],
            "started_at": dbt_step_meta_data["started_at"],
            "created_at": dbt_step_meta_data["created_at"],
            "is_complete": True,
        }
        return data


def _env_var(var: str, default: Optional[str] = None) -> str:
    """The env_var() function. Return the environment variable named 'var'.
    If there is no such environment variable set, return the default.
    If the default is None, raise an exception for an undefined variable.
    """
    if var in os.environ:
        return os.environ[var]

    if default is not None:
        return default

    msg = f"Environment variable required but not provided:'{var}'"
    raise EnvironmentError(msg)


_jinja_env: Optional[Environment] = None


def _setup_dbt_jinja_environment() -> Environment:
    global _jinja_env
    if _jinja_env is not None:
        return _jinja_env

    env = Environment(extensions=["jinja2.ext.do"])
    # When using env vars for Redshift port, it must be "{{ env_var('REDSHIFT_PORT') | as_number }}"
    # otherwise Redshift driver will complain, hence the need to add the "as_number" filter
    env.filters.update({"as_number": lambda x: x})
    env.globals["env_var"] = _env_var
    _jinja_env = env
    return _jinja_env


def _load_yaml(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def _load_yaml_with_jinja(path: str) -> Dict:
    loaded = _load_yaml(path)
    return _extract_jinja_values(loaded)


def _extract_jinja_values(data: T) -> T:
    env = _setup_dbt_jinja_environment()
    return _render_values_jinja(value=data, environment=env)


def _render_values_jinja(value: T, environment: Environment) -> T:
    """
    Traverses passed dictionary and render any string value using jinja.
    Returns copy of the dict with parsed values.
    """
    if isinstance(value, dict):
        parsed_dict = {}
        for key, val in value.items():
            parsed_dict[key] = _render_values_jinja(val, environment)

        return parsed_dict

    if isinstance(value, list):
        parsed_list = []
        for elem in value:
            rendered_value = _render_values_jinja(elem, environment)
            parsed_list.append(rendered_value)

        return parsed_list

    if isinstance(value, str):
        try:
            return environment.from_string(value).render()
        except TemplateError as error:
            log_exception("Jinja template error has occurred", ex=error)
            return value

    return value


def _load_dbt_core_assets(dbt_project_path: str) -> DbtCoreAssets:
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

    with open(os.path.join(dbt_project_path, "logs", "dbt.log"), "r") as logs_file:
        logs = logs_file.read()

    project = _load_yaml_with_jinja(os.path.join(dbt_project_path, "dbt_project.yml"))
    profile_dir = runs_info["args"]["profiles_dir"]
    profile_name = project["profile"]
    profile = _load_yaml_with_jinja(os.path.join(profile_dir, "profiles.yml"))[
        profile_name
    ]

    assets = DbtCoreAssets(runs_info, manifest, logs, profile)
    return assets


def _extract_environment(runs_info, profile):
    profile = _extract_profile(profile, runs_info)
    adapter = Adapter.from_profile(profile)
    environment = {"connection": adapter.connection(profile)}
    return environment


def _extract_profile(profile, runs_info):
    target = profile["target"]
    if "target" in runs_info["args"]:
        target = runs_info["args"]["target"]

    profile = profile["outputs"][target]
    return profile


def _extract_step_meta_data(runs_info, manifest, logs):
    dbt_step_meta_data = {
        "status_humanized": _extract_status(runs_info),
        "run_results": runs_info,
        "manifest": manifest,
        "duration": runs_info["elapsed_time"],
        "index": 1,
        "created_at": manifest["metadata"][
            "generated_at"
        ],  # manifest's generated_at is generated before the execution of the job starts
        "started_at": _calculate_started_time(runs_info),
        "logs": logs,
        "finished_at": runs_info["metadata"][
            "generated_at"
        ],  # generated_at is the time the runs_result.json artifact is generated, and that is when the run ends.
        "name": f"dbt {runs_info['args']['which']}",
    }
    return dbt_step_meta_data


def _extract_status(runs_info):
    status = "pass"
    n = next((x for x in runs_info["results"] if x["status"] != "pass"), None)
    if n is None:
        status = "fail"
    return status


def _calculate_started_time(runs_info):
    for run in runs_info["results"]:
        if run.get("timing") and "started_at" in run["timing"][0]:
            return run["timing"][0]["started_at"]

    return None


class Adapter(Enum):
    # This class represents supported adapters.
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    SPARK = "spark"
    OTHER = "other"

    @staticmethod
    def adapters() -> str:
        # String representation of all supported adapter names
        return ",".join([f"`{x.value}`" for x in list(Adapter)])

    @classmethod
    def from_profile(cls, profile):
        try:
            return Adapter[profile["type"].upper()]
        except KeyError:
            return Adapter.OTHER

    def extract_host(self, profile):
        if self == Adapter.SNOWFLAKE:
            return {"account": profile.get("account")}

        if self == Adapter.BIGQUERY:
            return {
                "project": profile.get("project"),
                "location": profile.get("location"),
            }

        if self == Adapter.REDSHIFT:
            host = profile["host"]
            return {
                "hostname": host,
                "host": host,
            }  # BE expect `hostname` but original is `host`

        if self == Adapter.SPARK:
            host = profile["host"]
            return {
                "hostname": host,
                "host": host,
            }  # BE expect `hostname` but original is `host`

        host = profile.get("host", "")
        return {"hostname": host, "host": host}

    def connection(self, profile):
        conn_type = profile["type"] if self == Adapter.OTHER else self.value
        conn = {"type": conn_type}
        conn.update(self.extract_host(profile))
        return conn
