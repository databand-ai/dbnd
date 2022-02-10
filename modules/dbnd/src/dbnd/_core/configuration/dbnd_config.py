import contextlib
import functools
import logging
import os
import typing

from typing import Any, List, Mapping, Optional, Union

import attr

from dbnd._core.configuration.config_readers import (
    _default_configuration_paths,
    parse_and_build_config_store,
    read_environ_config,
    read_from_config_files,
)
from dbnd._core.configuration.config_store import (
    _ConfigStore,
    _lower_config_name,
    merge_config_stores,
)
from dbnd._core.configuration.config_value import ConfigValue, fold_config_value
from dbnd._core.configuration.pprint_config import (
    pformat_all_layers,
    pformat_current_config,
)
from dbnd._core.context.bootstrap import dbnd_system_bootstrap
from dbnd._core.utils.basics.helpers import parse_bool
from dbnd._vendor.snippets.airflow_configuration import expand_env_var
from targets import target
from targets.values.builtins_values import STR_VALUE_TYPE


if typing.TYPE_CHECKING:
    from dbnd._core.configuration.config_store import _TConfigStore
    from dbnd._core.parameter.parameter_definition import ParameterDefinition
    from targets import Target

logger = logging.getLogger(__name__)


@attr.s(str=False)
class _ConfigLayer(object):
    name = attr.ib()  # type: str

    # the final view of config (standard merge strategy - per section)
    config = attr.ib()  # type: _TConfigStore

    # values of this layer only - the delta
    layer_config = attr.ib()  # type: _TConfigStore
    parent = attr.ib(default=None)  # type: Optional[_ConfigLayer]

    def get_config_value(self, section, key):
        # type: (str, str)->Optional[ConfigValue]
        section = self.config.get(_lower_config_name(section))
        if not section:
            return None
        return section.get(_lower_config_name(key))

    def merge_and_create_new_layer(
        self,
        name,  # type: str
        config_values,  # type: _TConfigStore
    ):
        new_config = merge_config_stores(self.config, config_values)
        return _ConfigLayer(
            name=name, parent=self, layer_config=config_values, config=new_config
        )

    def get_all_layers(self):
        trail = []
        current = self
        while current:
            trail.append(current)
            current = current.parent
        return trail

    @property
    def config_layer_path(self):
        return " <- ".join(l.name for l in self.get_all_layers())

    def __str__(self):
        return self.config_layer_path


@attr.s(str=False)
class DbndConfig(object):
    config_layer = attr.ib()  # type: _ConfigLayer
    # we need to initialize config with environment and config files first
    # if user already has config() calls , we will need to reapply his changes
    # on top of file and env configuration
    # User can run his config() before the first DatabandContext is called,
    # that means, we'll have to "replay his changes" on top of all this changes
    initialized_with_env = attr.ib(default=False)

    def _new_config_layer(self, config_values, source=None, priority=None):
        # let validate that we are initialized
        # user can call this function out of no-where, so we will create a layer, and will override it
        # the moment we create more layers on config.system_load
        dbnd_system_bootstrap()

        if not config_values:
            return self.config_layer
        if not isinstance(config_values, _ConfigStore):
            if not source:
                source = "{sig}".format(sig=id(config_values))
            config_values = parse_and_build_config_store(
                config_values=config_values, source=source, priority=priority
            )  # type: _ConfigStore

        source = source or config_values.source
        if not source:
            source = "{sig}".format(sig=id(config_values))
        return self.config_layer.merge_and_create_new_layer(
            name=source, config_values=config_values
        )

    @contextlib.contextmanager
    def __call__(self, config_values=None, source=None):
        new_layer = self._new_config_layer(config_values, source=source)
        with self.config_layer_context(config_layer=new_layer):
            yield self

    @contextlib.contextmanager
    def config_layer_context(self, config_layer):
        current_layer = self.config_layer
        current_initialized_with_env = self.initialized_with_env
        # this will create new layer
        try:
            self.config_layer = config_layer
            yield self
        finally:
            self.config_layer = current_layer
            self.initialized_with_env = current_initialized_with_env

    def set_values(self, config_values, source=None, priority=None):
        # type: (Union[ Mapping[str, Mapping[str, Any]], _ConfigStore], str, int)-> _ConfigLayer
        """
        Global override, changing current layout
        """
        self.config_layer = self._new_config_layer(
            config_values, source=source, priority=priority
        )
        return self.config_layer

    def set_from_config_file(self, config_path):
        # type: (Union[Target,str]) -> None
        """
        Load Configuration from config file
        """
        config_path = target(config_path)
        config_values = read_from_config_files([config_path])
        self.set_values(
            config_values=config_values, source=os.path.basename(str(config_path))
        )

    def set(self, section, key, value, priority=None, source=None):
        self.set_values({section: {key: value}}, source=source, priority=priority)

    def set_parameter(self, parameter, value, source=None):
        # type: (ParameterDefinition, object, str) -> None
        return self.set(parameter.task_family, parameter.name, value, source=source)

    def get_config_value(self, section, key):
        # type: (str, str)->Optional[ConfigValue]
        """
        Gets the value of the section/option using method.
        This is the function used by
        Returns default if value is not found.
        Raises an exception if the default value is not None and doesn't match the expected_type.
        """
        return self.config_layer.config.get_config_value(section, key)

    def get_multisection_config_value(self, sections, key):
        # type: (List[str], str) -> List[ConfigValue]
        """
        Looking for the the relevant value to the given key by scan all given sections in the config layer
         from top to bottom: where the current layer is the top and the parent layer is the bottom.

        return a list of collected values through the sections
        the list is ordered where the first value is from the lowest layer and the last is the highest layer
        """

        if len(sections) == 1:
            # we have it precalculated
            config_value = self.get_config_value(sections[0], key)
            if config_value:
                return [config_value]
            return []

        config_value_stack = []
        layer = self.config_layer  # type: _ConfigLayer
        # start to go from "child layers to parent"
        # section by section
        # Example:  ... -> layer.cmdline[MyTask] -> layer.cmdline[Task] -> layer.config[MyTask] -> ...
        while layer:
            for section in sections:
                # look for the value with highest priority
                # we are using "row" values from "delta"
                # we need to take care of priorities
                config_value = layer.layer_config.get_config_value(section, key)
                if config_value:
                    config_value_stack = fold_config_value(
                        stack=config_value_stack, lower=config_value
                    )
            layer = layer.parent  # type: _ConfigLayer
        return config_value_stack

    def get(self, section, key, default=None, expand_env=True):

        config_value = self.get_config_value(section=section, key=key)
        if config_value:
            value = config_value.value
            if expand_env:
                value = expand_env_var(value)
                value = STR_VALUE_TYPE._interpolate_from_str(value)
            return value
        return default

    def getboolean(self, section, key, **kwargs):
        return parse_bool(self.get(section, key, **kwargs))

    def getint(self, section, key, **kwargs):
        return int(self.get(section, key, **kwargs))

    def getfloat(self, section, key, **kwargs):
        return float(self.get(section, key, **kwargs))

    def load_system_configs(self, force=False):
        if self.initialized_with_env and not force:
            return

        # first, let read from files
        system_config_store = read_from_config_files(_default_configuration_paths())

        # now we can merge with environment values
        system_config_store.update(read_environ_config())

        # all configs will be added as one layer called 'system'
        self.set_values(system_config_store, source="system")
        self.initialized_with_env = True

    def __str__(self):
        return "Config[%s]" % self.config_layer.name

    def log_current_config(self, sections=None, as_table=False):
        logger.info(pformat_current_config(self, sections=sections, as_table=as_table))

    def log_layers(self, sections=None):
        logger.info(pformat_all_layers(self, sections=sections))

    @classmethod
    def build_empty(cls, name):
        return cls(
            config_layer=_ConfigLayer(
                name=name,
                config=_ConfigStore(),
                layer_config=_ConfigStore(),
                parent=None,
            )
        )


config = DbndConfig.build_empty(name="empty")


def config_deco(config_values, **deco_kwargs):
    """Do not use it in PY3, use config instead. Function decorator for PY2."""

    def decorator(method):
        @functools.wraps(method)
        def f(*args, **kwargs):
            with config(config_values, **deco_kwargs):
                return method(*args, **kwargs)

        return f

    return decorator
