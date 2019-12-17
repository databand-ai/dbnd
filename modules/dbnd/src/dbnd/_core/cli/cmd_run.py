from __future__ import print_function

import functools
import logging
import subprocess
import sys

import six

import attr

from dbnd._core.cli.click_utils import ConfigValueType, _help
from dbnd._core.cli.service_auto_completer import completer
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.configuration.pprint_config import pformat_config_store_as_table
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.log.config import configure_basic_logging
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.tracking.tracking_info_run import ScheduledRunInfo
from dbnd._vendor import click


logger = logging.getLogger(__name__)


def _to_conf(kwargs):
    return {k: str(v) for k, v in kwargs.items() if v is not None}


def build_dynamic_task(original_cls, new_cls_name):
    # Helper func to allow using --type-name feature
    original_name = original_cls.task_definition.task_family
    logger.info("Creating new class %s from %s", new_cls_name, original_name)
    attributes = {}  # {'_conf__task_family': original_cls.get_task_family()}
    if original_cls._conf__decorator_spec:
        conf__decorator_spec = attr.evolve(
            original_cls._conf__decorator_spec, name=new_cls_name
        )
        attributes["_conf__decorator_spec"] = conf__decorator_spec
    else:
        attributes["_conf__task_family"] = new_cls_name
    new_cls = TaskMetaclass(str(new_cls_name), (original_cls,), attributes)
    return new_cls


@click.command(context_settings=dict(help_option_names=[]))
@click.argument("task", autocompletion=completer.task(), required=False)
@click.option("--module", "-m", help="Used for dynamic loading of modules")
@click.option(
    "--set",
    "-s",
    "_sets",
    help="Set configuration value (task_name.task_parameter=value)",
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.task_param(),
)
@click.option(
    "--set-config",
    "-c",
    "_sets_config",
    help="Set configuration value (key=value)",
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.config_param(),
)
@click.option(
    "--set-root",
    "-r",
    "_sets_root",
    help="Set TASK parameter value (task_parameter=value)",
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.root_param(),
)
@click.option("--verbose", "-v", is_flag=True, help="Make logging output more verbose")
@click.option("--describe", is_flag=True, help="Describe current run")
@click.option(
    "--env",
    default="local",
    show_default=True,
    help="task environment: local/aws/aws_prod/gcp/prod",
)
@click.option("--parallel", is_flag=True, help="Run tasks in parallel")
@click.option("--conf-file", help="List of files to read from")
@click.option("--task-version", help="task version, directly affects task signature")
@click.option("--project-name", help="Name of this databand project")
@click.option("--name", help="Name of this run")
@click.option("--description", help="Description of this run")
@click.option(
    "--override",
    "-o",
    "_overrides",
    type=ConfigValueType(),
    multiple=True,
    help="Override configuration value (higher priority than any config source) key=value",
)
@click.option(
    "--help", "is_help", is_flag=True, help="Used for dynamic loading of modules"
)
@click.option("--run-driver", "run_driver", help="Running in remote mode")
@click.option("--task-name", "alternative_task_name", help="Name of this task")
@click.option(
    "--scheduled-job-name",
    "-sjn",
    help="Associate this run with this scheduled job (will be created if needed)",
)
@click.option(
    "--scheduled-date",
    "-sd",
    help="For use when setting scheduled-job-name",
    type=click.DateTime(),
)
@click.pass_context
def run(
    ctx,
    is_help,
    task,
    module,
    _sets,
    _sets_config,
    _sets_root,
    _overrides,
    verbose,
    describe,
    env,
    parallel,
    conf_file,
    task_version,
    project_name,
    name,
    description,
    run_driver,
    alternative_task_name,
    scheduled_job_name,
    scheduled_date,
):
    """
    Run a task or a DAG

    To see tasks use `dbnd show-tasks` (tab completion is available).
    """

    from dbnd._core.context.databand_context import new_dbnd_context, DatabandContext
    from dbnd._core.utils.structures import combine_mappings
    from dbnd import config

    task_name = task
    # --verbose, --describe, --env, --parallel, --conf-file and --project-name
    main_switches = dict(
        databand=dict(
            verbose=verbose,
            describe=describe,
            env=env,
            parallel=parallel,
            conf_file=conf_file,
            project_name=project_name,
        ),
        run=dict(name=name, description=description, is_archived=describe),
    )
    if task_version is not None:
        main_switches["task"] = {"task_version": task_version}

    cmd_line_config = parse_and_build_config_store(
        source="cli", config_values=main_switches
    )

    _sets = list(_sets)
    _sets_config = list(_sets_config)
    _sets_root = list(_sets_root)

    root_task_config = {}
    for _set in _sets_root:
        root_task_config = combine_mappings(left=root_task_config, right=_set)

    # remove all "first level" config values, assume that they are for the main task
    # add them to _sets_root
    for _set in _sets:
        for k, v in list(_set.items()):
            # so json-like values won't be included
            if "." not in k and isinstance(v, six.string_types):
                root_task_config[k] = v
                del _set[k]

    # --set, --set-config
    if _sets:
        cmd_line_config.update(_parse_cli(_sets, source="--set"))
    if _sets_config:
        cmd_line_config.update(_parse_cli(_sets_config, source="--set-config"))
    if _overrides:
        cmd_line_config.update(
            _parse_cli(_overrides, source="--set-override", override=True)
        )

    if cmd_line_config:
        logger.debug("CLI config: \n%s", pformat_config_store_as_table(cmd_line_config))
        config.set_values(cmd_line_config, source="cmdline")

    # double checking on bootstrap, as we can run from all kind of locations
    # usually we should be bootstraped already as we run from cli.
    dbnd_bootstrap()
    if not config.getboolean("log", "disabled"):
        configure_basic_logging(None)

    scheduled_run_info = None
    if scheduled_job_name:
        scheduled_run_info = ScheduledRunInfo(
            scheduled_job_name=scheduled_job_name, scheduled_date=scheduled_date
        )

    with new_dbnd_context(
        name="run", module=module
    ) as context:  # type: DatabandContext
        task_registry = get_task_registry()

        tasks = task_registry.list_dbnd_task_classes()
        completer.refresh(tasks)

        # modules are loaded, we can load the task
        task_cls = None
        if task_name:
            task_cls = task_registry.get_task_cls(task_name)
            if alternative_task_name:
                task_cls = build_dynamic_task(
                    original_cls=task_cls, new_cls_name=alternative_task_name
                )
                task_name = alternative_task_name

        # --set-root
        # now we can get it config, as it's not main task, we can load config after the configuration is loaded
        if task_cls is not None:
            if root_task_config:
                # adding root task to configuration
                config.set_values(
                    {task_cls.task_definition.task_config_section: root_task_config},
                    source="--set-root",
                )

        if is_help or not task_name:
            print_help(ctx, task_cls)
            return

        return context.dbnd_run_cmd_line(
            task_or_task_name=task_name,
            run_uid=run_driver,
            scheduled_run_info=scheduled_run_info,
        )


def print_help(ctx, task_cls):
    from dbnd._core.parameter.parameter_definition import _ParameterKind

    formatter = ctx.make_formatter()
    run.format_help(ctx, formatter)
    if task_cls:
        dl = []
        for (param_name, param_obj) in task_cls.task_definition.task_params.items():
            if param_obj.system or param_obj.kind == _ParameterKind.task_output:
                continue

            param_help = _help(param_obj.description)
            dl.append(("-r " + param_name, param_help))

        with formatter.section("Task"):
            formatter.write_dl(dl)
    click.echo(formatter.getvalue().rstrip("\n"), color=ctx.color)


def _parse_cli(configs, source, override=False):
    config_values_list = [
        parse_and_build_config_store(
            config_values=c, source=source, override=override, auto_section_parse=True
        )
        for c in configs
    ]
    return functools.reduce((lambda x, y: x.update(y)), config_values_list)


@dbnd_handle_errors(exit_on_error=False)
def dbnd_run_cmd(args):
    current_argv = sys.argv
    logger.info("Running dbnd run: %s", subprocess.list2cmdline(args))
    try:
        sys.argv = [sys.executable, "-m", "databand", "run"] + args

        return run(args=args, standalone_mode=False)
    finally:
        sys.argv = current_argv
