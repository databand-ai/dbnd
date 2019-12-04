import logging
import os

from collections import defaultdict
from datetime import datetime

import yaml

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.utils.string_utils import pluralize
from dbnd._core.utils.timezone import make_aware
from dbnd.api import scheduler_api_client
from dbnd.api.shared_schemas.scheduled_job_schema import (
    ScheduledJobSchemaV2,
    validate_cron,
)


logger = logging.getLogger(__name__)


class InvalidConfigException(Exception):
    pass


class SchedulerFileConfigLoader(object):
    def __init__(self, config_file=None):
        config.load_system_configs()
        if not config_file:
            self.config_file = config.get("scheduler", "config_file")
        self.active_by_default = config.get("scheduler", "active_by_default")

    def sync(self):
        if not self.config_file:
            return

        from_file = self.read_config(self.config_file)
        file_modified_time = make_aware(
            datetime.utcfromtimestamp(os.path.getmtime(self.config_file))
        )
        if not from_file:
            from_file = []

        self.validate(from_file)

        from_db = scheduler_api_client.get_scheduled_jobs(
            from_file_only=True, include_deleted=True
        )
        to_create, to_update, to_enable, to_disable = self.delta(
            from_db, from_file, file_modified_time
        )

        for s in to_create:
            scheduler_api_client.post_scheduled_job(s)
        for s in to_update + to_disable + to_enable:
            scheduler_api_client.patch_scheduled_job(s)

        if to_create or to_update or to_enable or to_disable:
            return {
                "created": len(to_create),
                "updated": len(to_update),
                "enabled (previously deleted)": len(to_enable),
                "disabled (deleted from file)": len(to_disable),
            }

    def read_config(self, config_file):
        if not config_file:
            return []

        with open(config_file, "r") as stream:
            try:
                config_entries = yaml.safe_load(stream)
                for i, s in enumerate(config_entries):
                    s["list_order"] = i
                return config_entries
            except yaml.YAMLError as exc:
                raise InvalidConfigException(
                    "failed to load scheduler config at %s: %s" % (config_file, exc)
                )

    def validate(self, config_entries):
        schema = ScheduledJobSchemaV2(strict=False)

        # validate entries against the schema and check the schedule_interval
        for config_entry in config_entries:
            _, errors = schema.load(config_entry)
            if errors:
                clean_errors = []
                for field, field_errors in errors.items():
                    clean_errors.append("%s: %s" % (field, ", ".join(field_errors)))
                config_entry["validation_errors"] = clean_errors

            config_entry["validation_errors"] = (
                config_entry["validation_errors"]
                if "validation_errors" in config_entry
                else []
            )

            if "schedule_interval" in config_entry:
                validation_error = validate_cron(config_entry["schedule_interval"])
                if validation_error:
                    config_entry["validation_errors"].append(
                        "Invalid schedule_interval: %s" % validation_error
                    )

        # check for duplicates
        group_by_name = defaultdict(lambda: [])
        for s in config_entries:
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
        log_message = ""
        for i, config_entry in enumerate(config_entries):
            if config_entry["validation_errors"]:
                config_entry["validation_errors"] = "\t" + "\n\t".join(
                    config_entry["validation_errors"]
                )
                log_message += "\n%s:\n%s" % (
                    config_entry["name"]
                    if "name" in config_entry and config_entry["name"]
                    else "<entry #%s name missing>" % i,
                    config_entry["validation_errors"],
                )
            else:
                config_entry["validation_errors"] = ""

        if log_message:
            logger.error("scheduler config file validation errors:%s" % log_message)

    def delta(self, from_db, from_file, file_modified_time):
        from_db_by_name = {s["name"]: s for s in from_db}
        from_file_by_name = {s["name"]: s for s in from_file}

        to_create = []
        to_update = []
        to_enable = []
        to_disable = []

        for s in from_file:
            if s["name"] in from_db_by_name:
                db_s = from_db_by_name[s["name"]]
                if db_s["deleted_from_file"]:
                    s["deleted_from_file"] = False
                    s["active"] = (
                        s["active"] if "active" in s else self.active_by_default
                    )
                    to_enable.append(s)
                elif self._key_diff(s, db_s):
                    # if we don't do this then if the user changes the active state from the ui
                    # it will constantly be overriden by the active state from the file
                    if db_s["update_time"] and db_s["update_time"] > file_modified_time:
                        del s["active"]

                    if self._key_diff(s, db_s):
                        to_update.append(s)
            else:
                s["from_file"] = True
                s["active"] = s["active"] if "active" in s else self.active_by_default
                to_create.append(s)

        for db_s in from_db:
            if db_s["name"] not in from_file_by_name and not db_s["deleted_from_file"]:
                to_disable.append(
                    {"name": db_s["name"], "deleted_from_file": True, "active": False}
                )

        return to_create, to_update, to_enable, to_disable

    def _key_diff(self, a, b):
        return any((key for key in a if key not in b or a[key] != b[key]))
