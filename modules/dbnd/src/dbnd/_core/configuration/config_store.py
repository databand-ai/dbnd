# © Copyright Databand.ai, an IBM Company 2022

import typing

from collections import OrderedDict
from typing import Optional, Set

import six

from dbnd._core.configuration.config_value import ConfigValue


# default type of section/values container
_config_dict_type = OrderedDict
_config_id = 0

# MARKERS:
CONFIG_REPLACE_SECTION_MARKER = "__config_replace_section"


def replace_section_with(dict_value):
    """Replace config section with passed values."""
    dict_value[CONFIG_REPLACE_SECTION_MARKER] = True
    return dict_value


def _lower_config_name(v):
    return v.lower()


# simple class that represents two level dict
class _ConfigStore(OrderedDict):
    def __init__(self, *args, **kwargs):
        OrderedDict.__init__(self, *args, **kwargs)
        self.source = None

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key.lower())
        except KeyError:
            self[key] = section = _config_dict_type()
            return section

    def get_config_value(self, section, key):
        # type: (str, str)->Optional[ConfigValue]
        section = self.get(_lower_config_name(section))
        if not section:
            return None
        return section.get(_lower_config_name(key))

    def set_config_value(self, section, key, value):
        self[_lower_config_name(section)][_lower_config_name(key)] = value

    def update(self, config_values):
        # type: (_ConfigStore) -> _ConfigStore
        for section, section_values in six.iteritems(config_values):
            # shallow copy of configuration
            section = _lower_config_name(section)
            replace_current_section = section_values.pop(
                CONFIG_REPLACE_SECTION_MARKER, None
            )

            if replace_current_section:
                # we can not just reuse section from right -> it's mutable objects
                self[section] = current_section = _config_dict_type()
            else:
                current_section = self[section]

            for key, value in six.iteritems(section_values):
                key = _lower_config_name(key)
                old_value = current_section.get(key)
                if old_value:
                    if old_value.priority > value.priority:
                        continue
                current_section[key] = value
        return self

    def as_value_dict(self, sections=None):
        # type: (Set[str])-> _config_dict_type
        return _config_dict_type(
            (
                section,
                _config_dict_type(
                    (key, str(config_value.value))
                    for key, config_value in six.iteritems(values)
                ),
            )
            for section, values in six.iteritems(self)
            if not sections or section in sections
        )


if typing.TYPE_CHECKING:
    # helping typehints system, our ordered dict is typed dict
    # not serializable in python < 3.7 (python bug)
    _TConfigStore = typing.Union[
        OrderedDict[str, OrderedDict[str, ConfigValue]], _ConfigStore
    ]


def merge_config_stores(config_left, config_right):
    # type : (_ConfigStore, _ConfigStore)-> _ConfigStore
    # known issue: non consisten copy on write
    # for example  c1.merge()  -> return c2.   c1.update() will affect c2 as well

    if not config_right:
        return config_left
    if not config_left:
        return config_right

    new_config = config_left.copy()
    # we copy only section that are changed
    for section in config_right.keys():
        new_config[section] = config_left[section].copy()

    # return original if no changes,
    # copy on write
    new_config.update(config_right)
    return new_config
