import functools
import logging
import os
import re

from collections.abc import Mapping
from configparser import ConfigParser
from typing import Any

import attr
import six

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.configuration.config_store import _ConfigStore
from dbnd._core.configuration.config_value import ConfigValue, ConfigValuePriority
from dbnd._core.configuration.environ_config import (
    get_dbnd_custom_config,
    get_dbnd_environ_config_file,
    in_quiet_mode,
    is_unit_test_mode,
)
from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils.project.project_fs import (
    databand_config_path,
    databand_system_path,
)
from dbnd._vendor.snippets.airflow_configuration import expand_env_var
from targets import LocalFileSystem, target
from targets.pipes import Text


logger = logging.getLogger(__name__)
_DBND_ENVIRON_RE = re.compile(r"^DBND__([\w\d_\-]+)__([\w\d_\-]+)$")
_SECTION_NAME_RE = re.compile(r"(.*)\.([^.]*)")


def _default_configuration_paths():
    # we always have "library config"
    yield databand_config_path("databand-core.cfg")

    system_config = os.path.expanduser("/etc/databand.cfg")
    if os.path.isfile(system_config):
        yield system_config

    # now we can start to look for project configs
    dbnd_project_config = get_dbnd_project_config()

    possible_locations = [
        databand_system_path("databand-system.cfg"),
        dbnd_project_config.dbnd_project_path("conf", "databand.cfg"),  # deprecated
        dbnd_project_config.dbnd_project_path("databand.cfg"),  # deprecated
        get_dbnd_custom_config(),
        dbnd_project_config.dbnd_project_path("project.cfg"),
    ]
    env_config = get_dbnd_environ_config_file()
    if env_config:
        possible_locations.append(env_config)
    for value in possible_locations:
        value = expand_env_var(value)
        if os.path.isfile(value):
            yield value

    user_config = expand_env_var("~/.dbnd/databand.cfg")
    if os.path.isfile(user_config):
        yield user_config

    if is_unit_test_mode():
        tests_config_path = databand_system_path("databand-test.cfg")
        if os.path.exists(tests_config_path):
            yield tests_config_path


def read_from_config_stream(config_fp, source="<stream>"):
    """
    Read config from config file (.ini, .cfg)
    """
    parser = ConfigParser()
    parser._read(config_fp, source)

    source = "config[{file_name}]".format(file_name=os.path.basename(str(source)))
    new_config = _ConfigStore()
    for section in parser.sections():
        for option in parser.options(section):
            value = parser.get(section, option)
            new_config.set_config_value(
                section, option, ConfigValue(value, source, require_parse=True)
            )

    return new_config


def read_from_config_file(config_file_target):
    """
    Read config from config file (.ini, .cfg)
    """

    # Check if config files are inside zip first - can happen if we run in fat wheel
    if (
        isinstance(config_file_target.fs, LocalFileSystem)
        and ".zip/" in config_file_target.path
    ):
        zip_file_path, config_path_inside_zip = config_file_target.path.split(".zip/")
        if os.path.exists(zip_file_path + ".zip"):
            import zipfile

            archive = zipfile.ZipFile(zip_file_path + ".zip", "r")
            if config_path_inside_zip not in archive.namelist():
                raise DatabandConfigError(
                    "Failed to read configuration file at %s, file not found!"
                    % config_file_target
                )
            archive = zipfile.ZipFile(zip_file_path + ".zip", "r")
            with archive.open(config_path_inside_zip) as file_io:
                return read_from_config_stream(
                    Text.pipe_reader(file_io), str(config_file_target)
                )

    if not config_file_target.exists():
        raise DatabandConfigError(
            "Failed to read configuration file at %s, file not found!"
            % config_file_target
        )
    with config_file_target.open("r") as fp:
        return read_from_config_stream(fp, str(config_file_target))


def read_from_config_files(config_files):
    files_to_load = [target(f) for f in config_files]
    configs = []

    if not in_quiet_mode():
        logger.info(
            "Reading configuration from: \n\t%s\n", "\n\t".join(map(str, files_to_load))
        )

    for f in files_to_load:
        try:
            configs.append(read_from_config_file(f))
        except DatabandConfigError:
            raise
        except Exception as ex:
            raise DatabandConfigError(
                "Failed to read configuration file at %s: %s" % (f, ex),
                nested_exceptions=ex,
            )

    merged_file_config = functools.reduce((lambda x, y: x.update(y)), configs)
    return merged_file_config


def read_environ_config():
    """
    Read configuration from process environment
    Every env var in following format will be added to config
    $DBND__SECTION__KEY=value  (please notice double underscore "__")
    :return:
    """
    return get_environ_config_from_dict(os.environ, "environ")


def get_environ_config_from_dict(env_dict, source_prefix):
    dbnd_environ = _ConfigStore()
    for key, value in six.iteritems(env_dict):
        if not key.startswith("DBND__"):
            continue
        # must have format DBND__{SECTION}__{KEY} (note double underscore)
        dbnd_key_var = _DBND_ENVIRON_RE.match(key)
        if dbnd_key_var:
            section, key = dbnd_key_var.group(1), dbnd_key_var.group(2)
            dbnd_environ.set_config_value(
                section,
                key,
                ConfigValue(
                    value,
                    source="{}[{}]".format(source_prefix, key),
                    require_parse=True,
                ),
            )
        else:
            # check that it's known name, or print error
            pass

    return dbnd_environ


def parse_and_build_config_store(
    source,
    config_values,
    auto_section_parse=False,
    priority=None,
    override=False,  # might be deprecated in favor of priority
    extend=False,
):
    # type:(str, Mapping[str, Mapping[str, Any]], bool, bool , bool, bool)->_ConfigStore
    """
    Read user defined values. Following format are supported:
        1. SomeTask.some_param [ParameterDefinition] : value
        2. { "section" : { "key" : "value" }}
        3 ? "SomeTask.some_param" [str]  : value
    """
    if isinstance(config_values, _ConfigStore):
        return config_values

    from dbnd._core.parameter.parameter_definition import ParameterDefinition

    new_config = _ConfigStore()
    new_config.source = source
    if not config_values:
        return new_config

    for section, section_values in six.iteritems(config_values):
        if isinstance(section, six.string_types):
            if auto_section_parse:
                m = _SECTION_NAME_RE.match(section)
                if m:  # section contains key!
                    section, key = m.group(1), m.group(2)
                    section_values = {key: section_values}

            if not isinstance(section_values, Mapping):
                raise DatabandConfigError(
                    "can't convert '%s' to configuration " % config_values
                )
        elif isinstance(section, ParameterDefinition):
            # this is parameter ->  Spark.jars = ["jars"]
            section_values = {section.name: section_values}
            section = section.task_config_section

        else:
            raise Exception("section='%s' not supported" % section)

        new_section = new_config[section]
        for key, value in six.iteritems(section_values):
            if key in new_section:
                raise Exception(
                    "multiple definition of {section}.{key} at {config}".format(
                        section=section, key=key, config=config_values
                    )
                )
            if isinstance(key, ParameterDefinition):
                key = key.name
            if not isinstance(value, ConfigValue):
                if priority is None:
                    priority = (
                        ConfigValuePriority.OVERRIDE
                        if override
                        else ConfigValuePriority.NORMAL
                    )
                value = ConfigValue(
                    value=value,
                    source=source,
                    require_parse=False,
                    priority=priority,
                    extend=extend,
                )
            else:
                # we can have override values without source
                if value.source is None:
                    value = attr.evolve(value, source=source)

                if extend:
                    value = attr.evolve(value, extend=extend)

            new_config.set_config_value(section, key, value)

    return new_config
