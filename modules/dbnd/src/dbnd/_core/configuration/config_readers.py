import functools
import logging
import os
import re

from collections import Mapping

import six

import attr

from dbnd._core.configuration.config_store import _ConfigStore
from dbnd._core.configuration.config_value import ConfigValue
from dbnd._core.configuration.environ_config import (
    get_dbnd_environ_config_file,
    in_quiet_mode,
    is_unit_test_mode,
)
from dbnd._core.errors import DatabandConfigError
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.utils.project.project_fs import (
    databand_config_path,
    databand_system_path,
    get_project_home,
)
from dbnd._vendor.snippets.airflow_configuration import expand_env_var
from targets import target


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
    possible_locations = [
        databand_system_path("databand-system.cfg"),
        os.path.join(get_project_home(), "conf", "databand.cfg"),
        os.path.join(get_project_home(), "databand.cfg"),
        os.path.join(get_project_home(), "project.cfg"),
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
        yield databand_system_path("databand-test.cfg")


def read_from_config_stream(config_fp, source="<stream>"):
    """
    Read config from config file (.ini, .cfg)
    """
    from backports.configparser import ConfigParser

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
        if not f.exists():
            raise DatabandConfigError(
                "Failed to read configuration file at %s, file not found!" % f
            )
        try:
            configs.append(read_from_config_file(f))
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
    dbnd_environ = _ConfigStore()
    for key, value in six.iteritems(os.environ):
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
                    value, source="environ[{}]".format(key), require_parse=True
                ),
            )
        else:
            # check that it's known name, or print error
            pass

    return dbnd_environ


def parse_and_build_config_store(
    source,
    config_values,
    override=False,
    auto_section_parse=False,
    set_if_not_exists_only=False,
):
    # type:(str, Mapping[str, Mapping[str, Any]], bool, bool , bool)->_ConfigStore
    """
    Read user defined values. Following format are supported:
        1. SomeTask.some_param [ParameterDefinition] : value
        2. { "section" : { "key" : "value" }}
        3 ? "SomeTask.some_param" [str]  : value
    """
    if isinstance(config_values, _ConfigStore):
        return config_values

    new_config = _ConfigStore()
    new_config.source = source
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
                value = ConfigValue(
                    value=value,
                    source=source,
                    require_parse=False,
                    override=override,
                    set_if_not_exists_only=set_if_not_exists_only,
                )
            else:
                # we can have override values without source
                if value.source is None:
                    value = attr.evolve(value, source=source)
            new_config.set_config_value(section, key, value)

    return new_config


def override(value):
    return ConfigValue(value=value, source=None, override=True)
