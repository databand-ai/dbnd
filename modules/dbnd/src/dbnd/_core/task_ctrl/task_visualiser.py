# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from dbnd._core.constants import TaskRunState
from dbnd._core.current import is_verbose
from dbnd._core.errors import DatabandBuildError, get_help_msg, show_exc_info
from dbnd._core.errors.errors_utils import log_exception, nested_exceptions_str
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from dbnd._core.task_run.task_run_error import task_call_source_to_str
from dbnd._core.utils.basics.text_banner import TextBanner, safe_string, safe_tabulate
from dbnd._vendor.termcolor import colored
from targets import DataTarget, InMemoryTarget, Target


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd_run.task.task import Task

logger = logging.getLogger(__name__)


def operators_list_str(operators, separator="\n\t"):
    operators = [o.task_id for o in operators]
    return separator.join(operators)


_TASK_FIELDS = [
    "task_env",
    "task_target_date",
    "task_version",
    "task_class_version",
    "task_enabled",
    "task_enabled_in_prod",
    "class_version",  # deprecated,
    "task_env",
    "task_band",
]


class FormatterVerbosity(object):
    LOW = 0
    NORMAL = 10
    HIGH = 20


def _important_value(value):
    return colored(value, color="red")


def _f_env(env):
    return _important_value(env) if env == "prod" else env


def _f_none_default(value, default):
    if value != default:
        return _important_value(value)
    return value


class _TaskBannerBuilder(TaskSubCtrl):
    """
    Describe how to build a banner for different scenarios.
    Also describe how to build each of the columns in the banner.
    todo: can be extracted to builder and director
    """

    def __init__(self, task: "_BaseTask", msg: str, color: str, verbose: bool = False):
        super(_TaskBannerBuilder, self).__init__(task)

        if verbose or is_verbose():
            verbosity = FormatterVerbosity.HIGH
        else:
            verbosity = FormatterVerbosity.NORMAL

        self.verbosity = verbosity

        self.banner = TextBanner(msg, color)
        self.table_director = _ParamTableDirector(task, self.banner)

    def build_tracking_banner(self, task_run=None, exc_info=None):
        # type: (_TaskBannerBuilder, TaskRun, ...) -> TextBanner
        """building the banner for tracking scenario - keep light as possible"""

        if task_run:
            self.add_tracker_info(task_run)

        self.table_director.add_params_table()

        self.add_stack_and_errors(exc_info, task_run, self.verbosity)

        return self.banner.get_banner_str()

    def add_tracker_info(self, task_run):
        if task_run.tracking_store.has_tracking_store("api", channel_name="web"):
            self.banner.column("TRACKER URL", task_run.task_tracker_url)

    def add_stack_and_errors(self, exc_info, task_run, verbosity):
        if (
            task_run
            and task_run.last_error
            and task_run.task_run_state != TaskRunState.SUCCESS
        ):
            self._add_last_error_info(exc_info=task_run.last_error.exc_info)

        elif exc_info:
            self._add_last_error_info(exc_info=exc_info)

        elif verbosity >= FormatterVerbosity.HIGH:
            self._task_create_stack()

    def _task_create_stack(self):
        if not hasattr(self.task, "task_call_source"):
            return

        task_call_source = task_call_source_to_str(self.task.task_call_source)
        if task_call_source:
            self.banner.column("TASK CREATED AT", value=task_call_source)

    def _add_last_error_info(self, exc_info=None):
        ex = exc_info[1]

        if show_exc_info(ex):
            # if we are showing internal task - we don't know how to "minimize" stack.
            # everything is databand.* ...
            isolate = not self.task.task_definition.full_task_family.startswith(
                "databand."
            )
            traceback_str = self.settings.tracking_log.format_exception_as_str(
                exc_info=exc_info, isolate=isolate
            )

            self.banner.column(
                colored("TRACEBACK", color="red", attrs=["bold"]),
                traceback_str,
                raw_name=True,
            )

        # some errors are empty
        if ex:
            self.banner.column(
                colored("ERROR:", color="red", attrs=["bold"]),
                "%s: %s" % (type(ex), str(ex)),
                raw_name=True,
            )

        self.banner.column(
            colored("HELP", attrs=["bold"]),
            get_help_msg(ex),
            raw_name=True,
            skip_if_empty=True,
        )
        if isinstance(ex, DatabandBuildError):
            self._task_create_stack()
        self.banner.column(
            colored("CAUSED BY", color="red", attrs=["bold"]),
            nested_exceptions_str(ex, limit=3),
            raw_name=True,
            skip_if_empty=True,
        )


## Building Params Table ##


class _ParamTableDirector(object):
    """
    Describes how to build the param table
    """

    def __init__(self, task, banner):
        # type: (Task, TextBanner) -> None
        self.task = task
        self.banner = banner
        self.header_builder = _ParamHeaderBuilder()

    def add_params_table(
        self,
        all_params=False,
        param_kind=True,
        param_type=True,
        param_format=False,
        param_source=False,
        param_section=False,
        param_default=False,
    ):
        # config
        self.kind = param_kind
        self.type = param_type
        self.format = param_format
        self.source = param_source
        self.section = param_section
        self.default = param_default

        """adds the param table to the banner"""
        from dbnd._core.task_build.task_results import ResultProxyTarget

        exclude = set(_TASK_FIELDS)  # excluding those

        # special treatment for result
        t_result = getattr(self.task, "result", None)

        if isinstance(t_result, ResultProxyTarget):
            self.banner.column("RESULT", t_result)
            exclude.add("result")

        params_data = []
        params_warnings = []
        # iterating over the params and building the data for the table
        for param_value in self.task._params:
            try:
                param_def = (
                    param_value.parameter.update_value_meta_conf_from_runtime_value(
                        param_value.value, self.task.settings.tracking
                    )
                )
                # filter switches
                if not all_params:
                    if param_def.name in exclude:
                        continue
                    if param_def.system:
                        continue

                # building a single row
                if param_def.value_meta_conf.log_preview:
                    param_value_preview = param_value.value
                else:
                    param_value_preview = "-"
                param_row = self.build_record(
                    param_def, param_value, param_value_preview
                )
                params_data.append(param_row)

                # extract param warnings
                if param_value and param_value.warnings:
                    params_warnings.extend(param_value.warnings)
            except Exception as ex:
                log_exception(
                    "Failed to calculate parameter value for %s.%s"
                    % (self.task.task_name, param_value.parameter.name),
                    ex,
                    non_critical=True,
                )

        if params_warnings:
            self.banner.column("BUILD WARNINGS:", "")
            self.banner.write("".join("\t%s\n" % pw for pw in params_warnings))

        if params_data:
            params_header = self.build_headers()
            params_table = safe_tabulate(
                tabular_data=params_data, headers=params_header
            )
            self.banner.column("PARAMS:", "")
            self.banner.write(params_table)

        self.banner.new_line()

    def build_record(self, p, param_meta, value):
        record_builder = _ParamRecordBuilder(self.task, p, value, param_meta)
        return self._build_row(record_builder)

    def build_headers(self):
        self.header_builder.reset()
        return self._build_row(self.header_builder)

    def _build_row(self, builder):
        """instructs the builder which of the columns to build"""
        builder.add_name()
        if self.kind:
            builder.add_kind()
        if self.type:
            builder.add_type()
        if self.format:
            builder.add_format()
        if self.source:
            builder.add_source()
        if self.default:
            builder.add_default()
        if self.section:
            builder.add_section()

        builder.add_value()

        return builder.row


class _ParamRecordBuilder(object):
    """Describe how to build each of the columns in the param table, from single param"""

    def __init__(self, task, definition, value, meta):
        self.task: _BaseTask = task
        self.definition = definition
        self.value = value if not definition.hidden else "***"
        self.meta = meta
        self.row = []

    def add_name(self):
        self.row.append(self.definition.name)

    def add_kind(self):
        p_kind = self._param_kind()
        self.row.append(p_kind)

    def _param_kind(self):
        if self.definition.is_output():
            p_kind = "output"
        elif not self.definition.load_on_build:
            p_kind = "input"
        else:
            p_kind = "param"
        return p_kind

    def add_type(self):
        self.row.append(self.definition.value_type_str)

    def add_source(self):
        source = self.meta.source if self.meta else ""
        self.row.append(source)

    def add_format(self):
        target_config = self.definition.target_config

        if isinstance(self.value, InMemoryTarget):
            target_config = "memory"

        if isinstance(self.value, DataTarget) and hasattr(self.value, "config"):
            target_config = self.value.config

        self.row.append(target_config)

    def add_section(self):
        section = self.definition.parameter_origin.task_family
        if section == self.task.get_task_family():
            section = ""

        self.row.append(section)

    def add_default(self):
        self.row.append(str(self.definition.default))

    def add_value(self):
        import dbnd  # noqa: 401 import dbnd before DataFrameValueType to avoid cyclic imports

        console_value_preview_size = (
            self.task.dbnd_context.settings.tracking_log.console_value_preview_size
        )

        if self.value is None:
            value_str = "@None"
        else:
            # We want to always use self.definition.to_str for panda's Dataframe value.
            # otherwise, the value blows up the log, and is not readable.
            if (
                self._param_kind() in ["input", "output"]
                and not self.definition.value_type_str == "DataFrame"
            ):
                value_str = TextBanner.f_io(self.value)
            else:
                value_str = self.definition.to_str(self.value)

        value_str = safe_string(value_str, console_value_preview_size)

        # add preview
        if isinstance(self.value, Target) and self.value.target_meta:
            preview_value = safe_string(
                self.value.target_meta.value_preview, console_value_preview_size
            )
            # we should add minimal preview
            if len(preview_value) < 100:
                value_str += " :='%s'" % preview_value

        if value_str and "\n" in value_str:
            # some simple heuristics around value
            extra_padding = " " * len("\t".join(map(str, self.row)))
            value_str = "".join(
                "\n%s%s" % (extra_padding, l) for l in value_str.split("\n")
            )
            value_str = "-->\n" + value_str

        self.row.append(value_str)


class _ParamHeaderBuilder(object):
    """Describe how to build each of the columns in the param table header"""

    def __init__(self):
        self.row = []

    def reset(self):
        self.row = []

    def add_name(self):
        self.row.append("Name")

    def add_kind(self):
        self.row.append("Kind")

    def add_type(self):
        self.row.append("Type")

    def add_source(self):
        self.row.append("Source")

    def add_format(self):
        self.row.append("Format")

    def add_section(self):
        self.row.append("Section")

    def add_default(self):
        self.row.append("Default")

    def add_value(self):
        self.row.append("-= Value =-")
