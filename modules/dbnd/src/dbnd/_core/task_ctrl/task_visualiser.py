import logging
import subprocess
import typing

import six

from dbnd._core.constants import SystemTaskName
from dbnd._core.current import is_verbose
from dbnd._core.errors import get_help_msg, show_exc_info
from dbnd._core.errors.errors_utils import log_exception, nested_exceptions_str
from dbnd._core.task.task import Task
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from dbnd._core.task_run.task_run_error import task_call_source_to_str
from dbnd._core.utils.basics.text_banner import TextBanner, safe_string, safe_tabulate
from dbnd._core.utils.structures import list_of_strings
from dbnd._vendor.termcolor import colored
from targets import DataTarget, InMemoryTarget, Target


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun

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

_MAX_VALUE_SIZE = 1500


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


class TaskVisualiser(TaskSubCtrl):
    def __init__(self, task):
        super(TaskVisualiser, self).__init__(task)

    def banner(self, msg, color=None, verbose=False, task_run=None, exc_info=None):
        try:
            b = TextBanner(msg, color)

            if verbose or is_verbose():
                verbosity = FormatterVerbosity.HIGH
            else:
                verbosity = FormatterVerbosity.NORMAL

            builder = _TaskBannerBuilder(task=self.task, banner=b, verbosity=verbosity)

            return builder.build_banner(
                task_run=task_run, exc_info=exc_info
            ).get_banner_str()
        except Exception as ex:
            log_exception(
                "Failed to calculate banner for '%s'" % self.task_id,
                ex,
                non_critical=True,
            )
            return msg + (" ( task_id=%s)" % self.task_id)


class _TaskBannerBuilder(TaskSubCtrl):
    def __init__(self, task, banner, verbosity, task_run=None):
        # type: (_TaskBannerBuilder, Task, TextBanner, int,  ...) -> None
        super(_TaskBannerBuilder, self).__init__(task)
        self.banner = banner
        self.task_run = task_run

        self.verbosity = verbosity

        self.is_driver_or_submitter = (
            self.task.task_name in SystemTaskName.driver_and_submitter
        )

    def build_banner(self, task_run=None, exc_info=None):
        # type: (_TaskBannerBuilder, TaskRun, ...) -> TextBanner
        b = self.banner

        self._add_main_info()

        if task_run:
            self._add_task_run_info(task_run)

        self._add_params_info()

        if self.verbosity >= FormatterVerbosity.HIGH:
            self._add_verbose_info()

        self.task._task_banner(self.banner, verbosity=self.verbosity)

        if task_run and task_run.last_error:
            self._task_create_stack()
            self._add_last_error_info(exc_info=task_run.last_error.exc_info)
        elif exc_info:
            self._task_create_stack()
            self._add_last_error_info(exc_info=exc_info)
        elif self.verbosity >= FormatterVerbosity.HIGH:
            self._task_create_stack()

        return b

    def _add_params_info(self):
        b = self.banner

        exclude = set(_TASK_FIELDS)

        # special treatment for result
        t_result = getattr(self.task, "result", None)
        from dbnd._core.decorator.schemed_result import ResultProxyTarget

        if isinstance(t_result, ResultProxyTarget):
            b.column("RESULT", t_result)
            exclude.add("result")

        params_data = []
        params_warnings = []
        all_info = self.verbosity >= FormatterVerbosity.HIGH

        relevant_params = []
        for p in self.params.get_params():
            if not all_info:
                # we don't want to show all this switches
                if p.name in exclude:
                    continue
                if p.system:
                    continue
            relevant_params.append(p)

        for p in relevant_params:
            value = self.params.get_value(p.name)
            param_meta = self.params.get_param_meta(p.name)
            target_config = p.target_config
            if isinstance(value, InMemoryTarget):
                target_config = "memory"

            if isinstance(value, DataTarget) and hasattr(value, "config"):
                target_config = value.config
            if p.is_output():
                p_kind = "output"
                value_str = b.f_io(value)
            elif not p.load_on_build:
                p_kind = "input"
                value_str = b.f_io(value)
            else:
                p_kind = "param"
                value_str = p.to_str(value)
            value_str = safe_string(value_str, _MAX_VALUE_SIZE)

            value_source = ""
            if param_meta:
                value_source = param_meta.source
                if param_meta.warnings:
                    params_warnings.extend(param_meta.warnings)
            type_handler = p.value_type_str
            param_data = [p.name, p_kind, type_handler, target_config, value_source]

            # add source task class of parameter
            if all_info:
                section = p.parameter_origin.get_task_family()
                if section == self.task.get_task_family():
                    section = ""
                default = str(p.default)  # TODO
                param_data += [default, section]

            # add preview
            if isinstance(value, Target) and value.target_meta:
                preview_value = safe_string(
                    value.target_meta.value_preview, _MAX_VALUE_SIZE
                )
                # we should add minimal preview
                if len(preview_value) < 100:
                    value_str += " :='%s'" % preview_value

            if value_str and "\n" in value_str:
                # some simple euristics around value
                extra_padding = " " * len("\t".join(map(str, param_data)))
                value_str = "".join(
                    "\n%s%s" % (extra_padding, l) for l in value_str.split("\n")
                )
                value_str = "-->\n" + value_str
            param_data.append(value_str)

            params_data.append(param_data)

            # config_params = [
            #     (p.name, value)
            #     for p, value in c._params.get_param_values()
            #     if value is not None
            # ]
            #
            # b.column(c.task_meta.task_family.upper(), b.f_params(config_params))
            # b.new_line()

        # 'Name' is missing,  header is aligned to the last column
        params_header = ["Name", "Kind", "Type", "Format", "Source"]
        if all_info:
            params_header.extend(["Default", "Class"])
        params_header.append("-= Value =-")

        if params_warnings:
            b.column("BUILD WARNINGS:", "")
            b.write("".join("\t%s\n" % pw for pw in params_warnings))

        if params_data:
            p = safe_tabulate(tabular_data=params_data, headers=params_header)

            b.column("PARAMS:", "")
            b.write(p)

        # b.new_line()
        # b.column("OUTPUTS", b.f_io(task_outputs_user))
        b.new_line()

    def _add_main_info(self):
        t = self.task  # type: Task
        b = self.banner

        from dbnd._core.task.task import DEFAULT_CLASS_VERSION

        task_params = [("task_id", t.task_id), ("task_version", t.task_version)]
        if t.task_class_version != DEFAULT_CLASS_VERSION:
            task_params.append(
                (
                    "task_class_version",
                    _f_none_default(t.task_class_version, DEFAULT_CLASS_VERSION),
                )
            )

        task_params.extend(
            [
                ("env", t.task_env.task_name),
                ("env_cloud", t.task_env.cloud_type),
                ("env_label", _f_env(t.task_env.env_label)),
                ("task_target_date", t.task_target_date),
            ]
        )
        b.column("TASK", b.f_simple_dict(task_params))
        if not t.ctrl.should_run():
            disable = [("task_enabled", t.task_enabled)]

            disable.append(("task_enabled_in_prod", t.task_enabled_in_prod))
            b.column("DISABLED", b.f_simple_dict(disable))

    def _add_task_run_info(self, task_run):
        # type: (TaskRun)-> None
        if not task_run:
            return
        b = self.banner

        time_fields = [("start", "%s" % task_run.start_time)]
        if task_run.finished_time:
            time_fields.append(("finished", "%s" % task_run.finished_time))
            task_duration = task_run.finished_time - task_run.start_time
            time_fields.append(("duration", "%s" % task_duration))

        b.column("TIME", b.f_simple_dict(time_fields))

        b.column("TRACKER", task_run.task_tracker_url)
        b.column_properties(
            "TASK RUN",
            [
                ("task_run_uid", task_run.task_run_uid),
                ("task_run_attempt_uid", task_run.task_run_attempt_uid),
                ("state", task_run.task_run_state),
            ],
        )
        if task_run.external_resource_urls:
            b.column_properties(
                "EXTERNAL",
                [(k, v) for k, v in six.iteritems(task_run.external_resource_urls)],
            )

        logs = [("local", task_run.log.local_log_file)]
        if task_run.log.remote_log_file:
            logs.append(("remote", task_run.log.remote_log_file))
        b.column("LOG", b.f_simple_dict(logs))

    def _task_create_stack(self):
        task_call_source = task_call_source_to_str(self.task.task_meta.task_call_source)
        if task_call_source and not self.is_driver_or_submitter:
            self.banner.column("CREATED BY", value=task_call_source)

    def _add_last_error_info(self, exc_info=None):
        ex = exc_info[1]
        b = self.banner

        if show_exc_info(ex):
            # if we are showing internal task - we don't know how to "minimize" stack.
            # everything is databand.* ...
            isolate = not self.task.task_definition.full_task_family.startswith(
                "databand."
            )
            traceback_str = self.settings.log.format_exception_as_str(
                exc_info=exc_info, isolate=isolate
            )

            b.column(
                colored("TRACEBACK", color="red", attrs=["bold"]),
                traceback_str,
                raw_name=True,
            )
        b.column(
            colored("ERROR MESSAGE", color="red", attrs=["bold"]),
            str(ex),
            raw_name=True,
        )

        b.column(
            colored("HELP", attrs=["bold"]),
            get_help_msg(ex),
            raw_name=True,
            skip_if_empty=True,
        )
        b.column(
            colored("CAUSED BY", color="red", attrs=["bold"]),
            nested_exceptions_str(ex, limit=3),
            raw_name=True,
            skip_if_empty=True,
        )

    def _add_verbose_info(self):
        b = self.banner
        t = self.task  # type: Task
        task_meta = self.task_meta
        relations = self.relations

        b.new_section()
        b.column("INPUTS USER", b.f_io(relations.task_inputs_user))
        b.column("INPUTS SYSTEM", b.f_struct(relations.task_inputs_system))
        b.new_section()

        b.column("TASK_BAND", t.task_band)
        b.column("OUTPUTS USER", b.f_io(relations.task_outputs_user))
        b.column("OUTPUTS SYSTEM", b.f_struct(relations.task_outputs_system))
        b.write("\n")

        b.column("SIGNATURE SOURCE", task_meta.task_signature_source)
        b.column(
            "TASK OUTPUTS SIGNATURE SOURCE", task_meta.task_outputs_signature_source
        )

    def _add_spark_info(self):
        b = self.banner

        b.new_section()
        try:
            spark_command_line = subprocess.list2cmdline(
                list_of_strings(self.task.application_args())
            )
            b.column("SPARK CMD LINE", spark_command_line)
        except Exception:
            logger.exception("Failed to get spark command line from %s" % self.task)
