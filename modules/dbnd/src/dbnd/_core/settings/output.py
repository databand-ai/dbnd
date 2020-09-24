from typing import Type

from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import Config
from targets.target_config import TargetConfig, file, parse_target_config
from targets.values import get_value_type_of_type
from targets.values.version_value import VersionAlias, VersionStr


class OutputConfig(Config):
    """(Advanced) Databand's core task's output behaviour"""

    _conf__task_family = "output"

    path_task = parameter(description="default path for every Task")[str]
    path_prod_immutable_task = parameter(
        description="format of the path to be used by Production Immutable tasks"
    )[str]

    hdf_format = (
        parameter.choices(["table", "fixed"])
        .help("Default format to save DataFrame to hdf")
        .value("fixed")
    )

    deploy_id = parameter(
        default=VersionAlias.context_uid,
        description="deploy prefix to use for remote deployments",
    )[VersionStr]

    def get_config(self, value_type):
        # type: (Type) -> TargetConfig

        type_handler = get_value_type_of_type(value_type)
        for possible_option in [str(type_handler), type_handler.config_name]:
            config_value = self.task_meta.get_task_config_value(key=possible_option)
            if config_value:
                return parse_target_config(config_value.value)
        return file.pickle
