import logging

from collections import defaultdict
from typing import List, Union

import yaml

from airflow import DAG

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.configuration.environ_config import ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.string_utils import clean_job_name, pluralize
from dbnd._core.utils.timezone import convert_to_utc
from dbnd._vendor.marshmallow import Schema, fields
from dbnd.api.shared_schemas.scheduled_job_schema import validate_cron
from dbnd_airflow.scheduler.scheduler_dags_provider import DbndSchedulerOperator


logger = logging.getLogger(__name__)


class InvalidConfigException(Exception):
    pass


yaml_implicit_resolvers_no_timestamp = {
    k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
    for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


class JobFromFileSchema(Schema):
    class Meta:
        strict = True

    name = fields.Str(required=True)
    cmd = fields.Str(required=True)
    schedule_interval = fields.Str(required=True)
    start_date = fields.DateTime(allow_none=False, required=True, format="iso")
    owner = fields.Str(allow_none=False)

    end_date = fields.DateTime(allow_none=True, format="iso")
    depends_on_past = fields.Boolean(allow_none=True)

    catchup = fields.Boolean(allow_none=True)
    retries = fields.Int(allow_none=True)

    list_order = fields.Integer(allow_none=True)
    active = fields.Boolean(allow_none=True)


class DbndAirflowDagsProviderFromFile(object):
    def __init__(self, config_file, active_by_default=False, default_retries=1):
        config.load_system_configs()
        self.config_file = config_file
        self.active_by_default = active_by_default
        self.default_retries = default_retries

    def get_dags(self):  # type: () -> List[DAG]
        jobs = self.read_config()
        dags = []
        for job in jobs:
            dag = self.job_to_dag(job)
            dags.append(dag)
        return dags

    def read_config(self):
        from_file = self._read_config()

        if not from_file:
            from_file = []

        from_file = self.load_and_validate(from_file)

        for s in from_file:
            s["from_file"] = True
            s["active"] = s["active"] if "active" in s else self.active_by_default

        return from_file

    def _read_config(self):

        with open(self.config_file, "r") as stream:
            try:
                try:
                    # this horrible hack is because otherwise the yaml loader will decide to be "helpful" and convert anything that looks like a datetime
                    # to a datetime object. This later collides with marshmallow trying to parse and validate what it expects to be a datetime strings
                    NoDatesSafeLoader = yaml.SafeLoader
                    original_yaml_implicit_resolvers = (
                        NoDatesSafeLoader.yaml_implicit_resolvers
                    )
                    NoDatesSafeLoader.yaml_implicit_resolvers = (
                        yaml_implicit_resolvers_no_timestamp
                    )
                    config_entries = yaml.load(stream, Loader=NoDatesSafeLoader)
                finally:
                    NoDatesSafeLoader.yaml_implicit_resolvers = (
                        original_yaml_implicit_resolvers
                    )

                if not config_entries:
                    return []

                if not isinstance(config_entries, list):
                    raise InvalidConfigException(
                        "failed to load scheduler config at %s: invalid yaml format. Expecting a list of objects. Parsed type: %s"
                        % (self.config_file, config_entries.__class__.__name__)
                    )

                for i, s in enumerate(config_entries):
                    s["list_order"] = i
                return config_entries
            except yaml.YAMLError as exc:
                raise InvalidConfigException(
                    "failed to load scheduler config at %s: %s"
                    % (self.config_file, exc)
                )

    def load_and_validate(self, file_entries):
        schema = JobFromFileSchema(strict=False)

        # validate entries against the schema and check the schedule_interval
        loaded_entries = []
        for file_entry in file_entries:
            loaded_entry, errors = schema.load(file_entry)
            loaded_entries.append(loaded_entry)
            if errors:
                clean_errors = []
                for field, field_errors in errors.items():
                    clean_errors.append("%s: %s" % (field, ", ".join(field_errors)))
                loaded_entry["validation_errors"] = clean_errors
            else:
                loaded_entry["validation_errors"] = []

            if "schedule_interval" in loaded_entry:
                validation_error = validate_cron(loaded_entry["schedule_interval"])
                if validation_error:
                    loaded_entry["validation_errors"].append(
                        "Invalid schedule_interval: %s" % validation_error
                    )

        # check for duplicates
        group_by_name = defaultdict(lambda: [])
        for s in loaded_entries:
            if "name" in s:
                group_by_name[s["name"]].append(s)
        duplicates = [entries for entries in group_by_name.values() if len(entries) > 1]
        for duplicate_set in duplicates:
            for entry in duplicate_set:
                entry["validation_errors"].append(
                    "%s other %s exist in the configuration file with the same name"
                    % (
                        len(duplicate_set) - 1,
                        pluralize("entry", len(duplicate_set) - 1, "entries"),
                    )
                )

        # format and log validation errors
        errors = []
        for i, config_entry in enumerate(loaded_entries):
            if not config_entry["validation_errors"]:
                continue

            for validation_error in config_entry["validation_errors"]:
                msg = "JOB '%s': %s" % (
                    config_entry.get("name", "<entry #%s name missing>" % i),
                    validation_error,
                )
                errors.append(msg)

        if errors:
            raise InvalidConfigException(
                "scheduler config file %s validation errors:\n\t%s"
                % (self.config_file, "\n\t".join(errors))
            )
        return loaded_entries

    def job_to_dag(self, job):  # type: (dict) -> Union[DAG, None]

        default_args = {}
        if job.get("depends_on_past"):
            default_args["depends_on_past"] = job.get("depends_on_past")

        start_date = convert_to_utc(job.get("start_date"))
        if start_date:
            default_args["start_day"] = start_date

        if job.get("end_date"):
            default_args["end_date"] = convert_to_utc(job.get("end_date"))

        if job.get("owner"):
            default_args["owner"] = job.get("owner")

        job_name = clean_job_name(job["name"])
        dag = DAG(
            "%s" % job_name,
            start_date=start_date,
            default_args=default_args,
            schedule_interval=job.get("schedule_interval", None),
            catchup=job.get("catchup", False),
        )

        DbndSchedulerOperator(
            task_id="launcher",
            dag=dag,
            retries=job.get("retries") or self.default_retries,
            scheduled_cmd=job["cmd"],
            scheduled_job_name=job_name,
            with_name=False,
            scheduled_job_uid=job.get("uid", None),
            shell=config.getboolean("scheduler", "shell_cmd"),
        )

        return dag


def get_dags_from_file():
    if environ_enabled(ENV_DBND_DISABLE_SCHEDULED_DAGS_LOAD):
        return None

    try:
        # let be sure that we are loaded
        config.load_system_configs()

        config_file = config.get("scheduler", "config_file")
        if not config_file:
            logger.info("No dags file has been defined at scheduler.config_file")
            return {}
        default_retries = config.getint("scheduler", "default_retries")
        active_by_default = config.getboolean("scheduler", "active_by_default")

        dags = DbndAirflowDagsProviderFromFile(
            config_file=config_file,
            active_by_default=active_by_default,
            default_retries=default_retries,
        ).get_dags()
        return {dag.dag_id: dag for dag in dags}
    except Exception as e:
        logging.exception("Failed to get dags from the file")
        raise e
