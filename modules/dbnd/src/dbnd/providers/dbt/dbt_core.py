# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import os.path
import shutil

from datetime import datetime
from typing import Dict, Optional, TypeVar

import attr
import yaml

from jinja2 import Environment, TemplateError, UndefinedError

from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.tracking.metrics import TRACKER_MISSING_MESSAGE, _get_tracker
from dbnd._core.utils.one_time_logger import get_one_time_logger
from dbnd.providers.dbt.dbt_adapters import Adapter


T = TypeVar("T")

logger = logging.getLogger(__name__)

DBT_CORE_SOURCE = "dbt_core"

################
#              #
#   DBT CORE   #
#              #
################


def collect_data_from_dbt_core(dbt_project_path: str):
    """
    Collect metadata for a single run of dbt core command.

    Args:
        dbt_project_path: Path (on local fs) where dbt project is located.
    """
    tracker = _get_tracker()

    if not tracker:
        message = TRACKER_MISSING_MESSAGE % ("collect_data_from_dbt_core",)
        get_one_time_logger().log_once(
            message, "collect_data_from_dbt_core", logging.WARNING
        )
        return

    phase: str = ""
    try:
        phase = "load"
        assets = _load_dbt_core_assets(dbt_project_path)

        phase = "extract-metadata"
        data = assets.extract_metadata()

        phase = "report"
        data["reported_from"] = DBT_CORE_SOURCE
        tracker.log_dbt_metadata(dbt_metadata=data)
    except Exception as e:
        logger.warning(
            "Failed to %s data from dbt core at path %s,continue execution",
            phase,
            dbt_project_path,
        )
        log_exception(f"Could not {phase} data from dbt core", e)


def _load_dbt_core_assets(dbt_project_path: str) -> "DbtCoreAssets":
    runs_info = _load_json_file(
        os.path.join(dbt_project_path, "target", "run_results.json")
    )
    manifest = _load_json_file(
        os.path.join(dbt_project_path, "target", "manifest.json")
    )
    logs = _read_and_truncate_logs(dbt_project_path)
    project = _load_yaml_with_jinja(os.path.join(dbt_project_path, "dbt_project.yml"))
    profile = _read_profile(runs_info, project)

    assets = DbtCoreAssets(runs_info, manifest, logs, profile)
    return assets


DEFAULT_DBT_CORE_PROJECT_NAME = "dbt_core_project"


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
            "job": {"name": self.job_name},
            "job_id": self.job_id,
            "started_at": dbt_step_meta_data.get("started_at"),
            "created_at": dbt_step_meta_data.get("created_at"),
            "finished_at": dbt_step_meta_data.get("finished_at"),
            "is_complete": True,
        }
        return data

    @property
    def dbt_invocation_id(self) -> str:
        return self.runs_info["metadata"]["invocation_id"]

    @property
    def dbt_core_project_name(self) -> Optional[str]:
        return self.manifest["metadata"].get("project_name")

    @property
    def dbt_core_project_id(self) -> Optional[str]:
        return self.manifest["metadata"].get("project_id")

    @property
    def job_id(self) -> str:
        return self.dbt_core_project_id or self.dbt_invocation_id

    @property
    def job_name(self) -> str:
        return self.dbt_core_project_name or DEFAULT_DBT_CORE_PROJECT_NAME


def _env_var(var: str, default: Optional[str] = None) -> str:
    """The env_var() function. Return the environment variable named 'var'.
    If there is no such environment variable set, return the default.
    If the default is None, raise an exception for an undefined variable.
    """
    if var in os.environ:
        return os.environ[var]

    if default is not None:
        return default

    return ""


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
        except UndefinedError as error:
            log_exception(
                "Jinja template error has occurred", ex=error, non_critical=True
            )
            return value
        except TemplateError as error:
            log_exception("Jinja template error has occurred", ex=error)
            return value

    return value


def _load_json_file(file_path: str) -> Dict:
    with open(file_path, "r") as file:
        return json.loads(file.read())


def _read_profile(runs_info: Dict, project: Dict) -> Dict:
    try:
        profile_dir = runs_info["args"]["profiles_dir"]
        profile_name = project["profile"]
        profiles_file_path = os.path.join(profile_dir, "profiles.yml")
        profiles = _load_yaml_with_jinja(profiles_file_path)
        return profiles[profile_name]
    except KeyError as e:
        log_exception(
            "dbt profile information not found in runs_info or project.", ex=e
        )
        raise


def _read_and_truncate_logs(dbt_project_path: str) -> str:
    dbt_log_path = os.path.join(dbt_project_path, "logs", "dbt.log")
    with open(dbt_log_path, "r") as logs_file:
        logs = logs_file.read()

    # Copy original log file to new location, only for backup purposes.
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
    new_file_name = f"dbt.log.{current_date}"
    new_file_path = os.path.join(dbt_project_path, "logs", new_file_name)
    shutil.copy(dbt_log_path, new_file_path)

    # Truncate the original file.
    #  If there are open handles to it, they will continue writing, but to empty file
    with open(dbt_log_path, "w") as _:
        pass

    return logs


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


def _extract_status(runs_info) -> str:
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
