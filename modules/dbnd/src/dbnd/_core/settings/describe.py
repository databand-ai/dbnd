from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import Config


class DescribeConfig(Config):
    """(Advanced) Databand's --describe behaviour"""

    _conf__task_family = "describe"

    dry = parameter(
        default=False, description="Describe without pushing to databand-web"
    )[bool]

    no_checks = parameter(
        default=False, description="Describe without doing copleteness and other checks"
    )[bool]
    no_tree = parameter(
        default=False, description="Describe without showing tasks tree"
    )[bool]

    console_value_preview_size = parameter(
        description="Maximum length of string previewed in TaskVisualiser"
    )[int]
