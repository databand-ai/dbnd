from typing import List

from dbnd._core.configuration.config_store import _ConfigStore
from dbnd._core.configuration.config_value import ConfigValue
from dbnd._core.configuration.dbnd_config import DbndConfig
from dbnd._core.configuration.pprint_config import pformat_current_config
from targets.target_config import parse_target_config


class MultiSectionConfig(object):
    def __init__(self, config, sections):
        # type: (DbndConfig, List[str]) -> None
        self.config = config
        self.sections = sections

    def update_section(self, section, param_values, source):
        # we take values using names only
        defaults_store = _ConfigStore()
        for key, value in param_values:
            previous_value = self.config.get(section, key)
            if previous_value != value:
                cf = ConfigValue(value=value, source=source)
                defaults_store.set_config_value(section, key, cf)

        # we apply set on change only in the for loop, so we can optimize and not run all these code
        if defaults_store:
            self.config.set_values(config_values=defaults_store, source=source)

    def get_config_value(self, key):
        """
        If we have any override in the config -> use them!
        otherwise, return first value
        in case we have value in better section, but override in low priority section
        override wins!
        """
        best_config_value = None
        for section in self.sections:
            config_value = self.config.get_config_value(section, key)
            # first override win!
            if config_value:
                if config_value.override:
                    return config_value
                if best_config_value is None:
                    best_config_value = config_value
        return best_config_value

    def get_param_config_value(self, param_def):
        try:
            cf_value = self.get_config_value(key=param_def.name)
            if cf_value:
                return cf_value

            if param_def.config_path:
                return self.config.get_config_value(
                    section=param_def.config_path.section, key=param_def.config_path.key
                )
            return None
        except Exception as ex:
            raise param_def.parameter_exception("read configuration value", ex)

    @property
    def config_log(self):
        return "config for sections({config_sections}): {config}".format(
            config=pformat_current_config(
                self.config, sections=self.sections, as_table=True
            ),
            config_sections=self.sections,
        )

    def get_section(self, section_name):
        return self.config.config_layer.config.get(section_name)

    @property
    def layer(self):
        return self.config.config_layer

    def set_values(
        self, config_values, source=None, override=False, merge_settings=None
    ):
        self.config.set_values(config_values, source, override, merge_settings)

    def iter_sections(self):
        for section_name in self.sections:
            section = self.get_section(section_name)
            if section:
                yield section_name, section
