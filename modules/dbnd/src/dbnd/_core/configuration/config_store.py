import typing

from collections import OrderedDict
from typing import Optional

import six

import attr

from dbnd._core.configuration.config_value import ConfigValue


# default type of section/values container
_config_dict_type = OrderedDict
_config_id = 0


def _lower_config_name(v):
    return v.lower()


@attr.s
class _ConfigMergeSettings(object):
    on_non_exists_only = attr.ib(default=False)
    on_change_only = attr.ib(default=False)
    replace_section = attr.ib(default=False)


class ConfigMergeSettings(object):
    default = _ConfigMergeSettings()
    on_change_only = _ConfigMergeSettings(on_change_only=True)
    on_non_exists_only = _ConfigMergeSettings(on_non_exists_only=True)


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

    def merge(self, config_values, merge_settings=None):
        # type : (_ConfigStore, _ConfigMergeSettings)-> _ConfigStore
        # known issue: non consisten copy on write
        # for example  c1.merge()  -> return c2.   c1.update() will affect c2 as well

        if not config_values:
            return self
        new_config = self.copy()
        # we copy only section that are changed
        for section in config_values.keys():
            new_config[section] = self[section].copy()
        new_config.update(config_values, merge_settings)
        return new_config
        # return original if no changes,
        # copy on write

    def update(self, config_values, merge_settings=None):
        # type : (_ConfigStore, _ConfigMergeSettings) -> _ConfigStore
        merge_settings = merge_settings or ConfigMergeSettings.default
        for section, section_values in six.iteritems(config_values):
            # shallow copy of configuration
            section = _lower_config_name(section)
            replace_current_section = section_values.get(
                "_replace_section", merge_settings.replace_section
            )

            if replace_current_section:
                self[section] = current_section = _config_dict_type()
            else:
                current_section = self[section]

            for key, value in six.iteritems(section_values):
                key = _lower_config_name(key)
                assert isinstance(
                    value, ConfigValue
                ), "{section}.{key} expected to be ConfigValue".format(
                    section=section, key=key
                )
                old_value = current_section.get(key)
                # if old value is override, we will "override" only if the new one is override
                if old_value:
                    if old_value.override and not value.override:
                        continue
                    if (
                        merge_settings.on_non_exists_only
                        or value.set_if_not_exists_only
                    ):
                        continue
                    if merge_settings.on_change_only and old_value.value == value.value:
                        continue
                current_section[key] = value
        return self

    def as_value_dict(self, sections=None):
        # type: (Set[str])-> _config_dict_type
        return _config_dict_type(
            (
                section,
                _config_dict_type(
                    (key, config_value.value)
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
