from __future__ import absolute_import

import sys

from dbnd._core.errors import (
    DatabandBuildError,
    DatabandConfigError,
    DatabandError,
    DatabandRuntimeError,
    TaskClassAmbigiousException,
)
from dbnd._core.errors.errors_utils import safe_value
from dbnd._core.errors.friendly_error import (
    api,
    config,
    execute_engine,
    executor_k8s,
    graph,
    targets,
    task_build,
    task_execution,
    task_parameters,
    task_registry,
    tools,
)
from dbnd._core.errors.friendly_error.helpers import (
    _band_call_str,
    _run_name,
    _safe_target,
    _safe_task_family,
)
from dbnd._core.utils.basics.format_exception import format_exception_as_str


def task_found_in_airflow_dag(root_task):
    return DatabandError(
        "Task '%s' implementation has been discovered via DAGs loading" % root_task,
        help_msg="Your task %s were loaded via dags folder, currently we don't support that.\n"
        "You should define your tasks not in airflow/dag folder, but inside your project.\n"
        "Use module key in a [databand] section of config file ( $DBND__DATABAND__MODULE )"
        % root_task,
        show_exc_info=False,
    )


def task_has_not_complete_but_all_outputs_exists(task):
    return DatabandRuntimeError(
        "Something wrong, task %s has been executed, "
        "but _complete function returns False while all outputs exist! "
        % _safe_task_family(task),
        help_msg="Check your implementation of %s, validate that you _complete override is correct",
        show_exc_info=False,
    )


def task_has_missing_outputs_after_execution(task, missing_str):
    return DatabandRuntimeError(
        "Task %s has been executed, but some outputs are missing!\n\t%s"
        % (_safe_task_family(task), missing_str),
        help_msg="Check your implementation of %s, validate that all outputs has been written. "
        "If it's directory, validate that you have _SUCCESS flag "
        "(self.your_task_output.mark_success())" % _run_name(task),
        show_exc_info=False,
    )


def task_data_source_not_exists(task, missing, downstream=None):
    if downstream:
        tasks = ",".join(map(str, downstream))
        dependent = "Tasks that depend on this input are: %s\n" % tasks
    else:
        dependent = ""
    if len(missing) == 1:
        missing_target = missing[0]

        from targets.dir_target import DirTarget

        if (
            isinstance(missing_target, DirTarget)
            and missing_target.folder_exists()
            and missing_target.flag_target
        ):
            # we are missing flag!
            return DatabandRuntimeError(
                "Data source '%s' success flag is missing! %s"
                % (missing_target.flag_target, dependent),
                help_msg="Check that SUCCESS flag exists and your configurations is ok. "
                "You can override flag check by \n"
                "1. adding '[noflag]' to your path: '%s[noflag]' \n"
                "2. --PARAMETER-target '[noflag]' \n"
                "3. Define parameter using 'parameter.folder.with_flag(None)' \n"
                "4 .Create the flag if you think that the input is ok" % missing_target,
                show_exc_info=False,
            )
        return DatabandRuntimeError(
            "Task input at location '%s' is missing! %s" % (missing_target, dependent),
            help_msg="Validate that this data exists. "
            "This string considered as a path, as it defined as 'data' in our system",
            show_exc_info=False,
        )

    if len(missing) > 5:
        missing_msg = "%s... (%s files)" % (missing[:10], len(missing))
    else:
        missing_msg = ",".join(map(str, missing))

    return DatabandRuntimeError(
        "Data source '%s' is missing! %s" % (missing_msg, dependent),
        help_msg="Check that file exists and your configurations is ok. "
        "If it's directory, validate that you have _SUCCESS flag, "
        "or override that via target config ('noflag')",
        show_exc_info=False,
    )


def ambiguous_task(full_task_name):
    return TaskClassAmbigiousException(
        "Task %r is ambiguous, you have more than one task with the same name!"
        % full_task_name,
        help_msg="Check whether this module contains more than one methods with the same name. "
        "Please use full reference (like 'a.b.c') ",
        show_exc_info=False,
    )


def no_matching_tasks_in_pipeline(tasks, tasks_regexes):
    all_tasks_names = ",".join([t.task_id for t in tasks])
    return DatabandConfigError(
        "None of '%s' tasks have been found at current pipeline!" % tasks_regexes,
        help_msg="check your run.selected_tasks_regex switch, "
        "select one of following tasks: %s" % all_tasks_names,
        show_exc_info=False,
    )


def dag_with_different_contexts(task_id):
    return DatabandRuntimeError(
        "The task '%s' isn't part of the current context!" % task_id,
        help_msg="The task '%s' isn't part of the current context! \n"
        "Make sure you did not fiddle with internal APIs" % task_id,
        show_exc_info=False,
    )


def dbnd_module_not_found_tip(module):
    if str(module).startswith("dbnd"):
        dbnd_module = module.split(".")[0]
        return "Do you have '%s' installed?" % (dbnd_module.replace("_", "-"))
    return ""


def failed_to_import_user_module(ex, module, description):
    s = format_exception_as_str(sys.exc_info())
    msg = "Module '%s' can not be loaded: %s" % (
        module,
        dbnd_module_not_found_tip(module),
    )
    return DatabandError(
        "%s exception: %s." % (msg, s),
        help_msg=" Databand is trying to load user module '%s' as required by %s: \n "
        "Probably, it has compile errors or not exists." % (module, description),
        show_exc_info=False,
    )


def unknown_args_in_task_call(parent_task, cls, call_args, call_repr):
    return DatabandBuildError(
        "You are trying to create %s from %s with %s *args, please use named arguments only: %s"
        % (_run_name(cls), _band_call_str(parent_task), len(call_args), call_repr),
        show_exc_info=True,
        help_msg="Check your %s logic" % _band_call_str(parent_task),
    )


def failed_to_create_task(exc_desc, nested_exceptions):
    msg = "Failed to create task %s" % exc_desc
    if nested_exceptions:
        msg += ": "
        msg += ",".join([str(e) for e in nested_exceptions[:3]])
        if len(nested_exceptions) > 3:
            msg += " (showing 3 errors out of %s)" % len(nested_exceptions)
    return DatabandBuildError(
        msg, show_exc_info=False, nested_exceptions=nested_exceptions
    )


def failed_to_convert_value_to_target(value):
    return DatabandBuildError(
        "Can't convert '%s' of type %s to target." % (safe_value(value), type(value)),
        help_msg="You can convert only values with type of "
        "Target, Tasks and strings to the Path/PathStr/Target",
    )


def failed_to_calculate_task_parameter_value(ex, task_family, p_name, exc_desc):
    return DatabandBuildError(
        "Failed to process parameter %s for %s: %s" % (p_name, exc_desc, ex),
        nested_exceptions=[ex],
        help_msg="Please validate your config/commandline/.band that creates runs %s"
        % exc_desc,
    )


def failed_to_read_pandas(ex, target):
    return DatabandError(
        "There is an error while reading {target}: {ex}".format(
            target=_safe_target(target), ex=ex
        ),
        nested_exceptions=[ex],
    )


def failed_to_set_index(ex, df, set_index, target):
    return DatabandError(
        "Failed to set index to '{set_index}' "
        "for data frame with columns {columns} "
        "while reading from {target}: {ex}".format(
            set_index=set_index, columns=df.columns, target=_safe_target(target), ex=ex
        ),
        nested_exceptions=[ex],
    )


def failed_to_read_target_as_task_input(ex, task, parameter, target):
    return DatabandError(
        "Failed to read '{task.task_name}.{p.name}' from "
        "'{target}' as format '{target.config}' to {p.value_type.type_str}: {ex}"
        "".format(p=parameter, target=target, task=task, ex=ex),
        nested_exceptions=ex,
    )


def failed_to_read_task_input(ex, task, parameter, target):
    return DatabandError(
        "Failed to read '{task.task_name}.{p.name}' from "
        "'{target}' to {p.value_type.type_str}: {ex}"
        "".format(p=parameter, target=target, task=task, ex=ex),
        nested_exceptions=ex,
    )


def failed_to_write_task_output(ex, target, value_type):
    return DatabandError(
        "Failed to write '{target.name}' to '{target} {target.config}  ({value_type}): {ex}"
        "".format(value_type=value_type, target=target, ex=ex),
        nested_exceptions=ex,
    )


def failed_to_write_pandas(ex, target):
    return DatabandError(
        "There is an error while writing to {target} {ex}".format(
            target=_safe_target(target), ex=ex
        ),
        nested_exceptions=ex,
    )


def marshaller_no_merge(marshaller, target, partitions):
    return DatabandError(
        "Can't merge {p_len} partitions on read from {target} "
        "as current marshaller {marshaller} doesn't support merge funcitonality".format(
            p_len=len(partitions), target=target, marshaller=marshaller
        )
    )


def airflow_bad_user_configuration(ex, file_path):
    return DatabandConfigError(
        "Error while trying to load additional airflow configuration from %s"
        % file_path,
        help_msg="Please make sure that the configuration file %s does exist."
        % file_path,
        nested_exceptions=ex,
        show_exc_info=False,
    )


def airflow_versioned_dag_missing(command):
    return DatabandError(
        "Could not run '%s', dbnd-airflow-versioned-dag is not installed." % command,
        help_msg="Please run 'pip install dbnd-airflow-versioned-dag' in order to run '%s'."
        % command,
        show_exc_info=False,
    )
