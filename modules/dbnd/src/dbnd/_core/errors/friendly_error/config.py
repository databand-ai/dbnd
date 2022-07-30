# © Copyright Databand.ai, an IBM Company 2022

import typing

from typing import Any, Set

import dbnd

from dbnd._core.errors import DatabandConfigError, ParseParameterError


if typing.TYPE_CHECKING:
    from dbnd._core.parameter.parameter_definition import ParameterDefinition


def wrong_func_attr_format(attr_path):
    return DatabandConfigError(
        "Wrong format for code path:  got '%s' instead of  module_ref.module.name"
        % attr_path
    )


def task_name_and_from_are_the_same(section, param):
    return DatabandConfigError(
        "Your [%s] defined using _type=%s. "
        "System can't handle recursive type resolving. "
        "Probably you are missing the module that has '%s' defined. "
        % (param, section, section),
        help_msg="Please check your configuration section [%s] "
        "or try to install package with %s" % (section, section),
    )


def env_section_not_found(section):
    return DatabandConfigError(
        "%s is listed in your `environments` list but a config section with this name could not be found"
        % section,
        help_msg="Please add a config section [%s] that configures the environment or remove it from the environment list"
        % section,
    )


def dbnd_root_local_not_defined(env):
    return DatabandConfigError(
        "You environment '{env}' definition doesn't have dbnd_local_root defined".format(
            env=env
        ),
        help_msg="Please define it at [{env}]. This path is used for all local internal dbdn system outputs".format(
            env=env
        ),
    )


def no_credentials():
    return DatabandConfigError(
        "No credentials are defined for the cloud environment you are trying to use",
        help_msg="Please configure the connection for your cloud env's conn_id (if conn_id is unset it will be [aws|gcp|azure]_default).\n"
        + "See: https://databand.readme.io/docs/integrations",
    )


def scheduled_job_missing_param(field):
    return DatabandConfigError("Scheduled job %s must be defined" % field)


def scheduled_job_exists(name):
    return DatabandConfigError(
        "Scheduled job named '%s' already exists" % name,
        help_msg="If you wish to update it add --update to the cli command",
    )


def scheduled_job_not_exists(name):
    return DatabandConfigError(
        "Scheduled job named '%s' does not exists and as such cannot be updated" % name
    )


def scheduled_job_invalid_interval(schedule_interval):
    from dbnd.api.shared_schemas.scheduled_job_schema import SCHEDULE_INTERVAL_PRESETS

    return DatabandConfigError(
        "The schedule_interval '%s' is not valid" % schedule_interval,
        help_msg="Valid values are any valid cron expression or one of the presets: %s"
        % ", ".join(SCHEDULE_INTERVAL_PRESETS),
    )


def wrong_store_name(name):
    return DatabandConfigError(
        "Unsupported tracking store: '{}', use one of file/console/api".format(name),
        help_msg="Please check you configuration at [core] tracker.",
    )


def wrong_tracking_api_name(name):
    return DatabandConfigError(
        "Unsupported tracking api: '{}', use one of web/db".format(name),
        help_msg="Please check you configuration at [core] tracker_api.",
    )


def choice_error(param, value, choices):
    # type: (ParameterDefinition,Any, Set[Any])->ParseParameterError
    return ParseParameterError(
        "{param}: {value} is not a valid choice from {choices}".format(
            param=param, value=value, choices="/".join(map(str, choices))
        )
    )


def empty_string_validator(param):
    # type: (ParameterDefinition)->ParseParameterError
    return ParseParameterError(
        "{param} has empty string value".format(param=param),
        help_msg="Please check your configuration for {}".format(param),
    )


def missing_module(module, reason=None):
    return DatabandConfigError(
        "'{module}' module is not found. {reason}".format(
            module=module, reason=reason or ""
        ),
        help_msg="Please, `pip install '{module}=={version}'`,".format(
            module=module, version=dbnd.__version__
        ),
    )
