from __future__ import print_function

import functools
import logging

import attr
import six

from dbnd._core.cli.click_utils import ConfigValueType, _help
from dbnd._core.cli.service_auto_completer import completer
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.configuration.config_value import ConfigValuePriority
from dbnd._core.configuration.environ_config import tracking_mode_context
from dbnd._core.configuration.pprint_config import pformat_config_store_as_table
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.log.config import configure_basic_logging
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.tracking.schemas.tracking_info_run import ScheduledRunInfo
from dbnd._core.utils.basics.dict_utils import filter_dict_remove_false_values
from dbnd._vendor import click
from dbnd._vendor.click_tzdatetime import TZAwareDateTime


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
@click.option(
    "--module",
    "-m",
    help="Define a path of a module where DBND can search for a task or pipeline",
)
@click.option(
    "--set",
    "-s",
    "_sets",
    help="Set a configuration value (task_name.task_parameter=value)",
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.task_param(),
)
@click.option(
    "--set-config",
    "-c",
    "_sets_config",
    help="Set configuration a value (key=value). Example: --set run.name=my_run",
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.config_param(),
)
@click.option(
    "--extend",
    "-x",
    "_extend",
    help="extend configuration value. The configuration values must support extending. Example: --extend core.tracker='["
    '"debug"]\'',
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.config_param(),
)
@click.option(
    "--set-root",
    "-r",
    "_sets_root",
    help="Set or override a task parameter value (task_parameter=value)",
    type=ConfigValueType(),
    multiple=True,
    autocompletion=completer.root_param(),
)
@click.option(
    "--verbose", "-v", count=True, help="Make the logging output more verbose"
)
@click.option(
    "--print-task-band", is_flag=True, help="Print task_band in the logging output."
)
@click.option("--describe", is_flag=True, help="Describe the current run")
@click.option(
    "--env",
    default="local",
    show_default=True,
    help="task environment: local/aws/aws_prod/gcp/prod",
)
@click.option("--parallel", is_flag=True, help="Run tasks in parallel")
@click.option("--conf-file", help="List of files to read from")
@click.option("--task-version", help="Task version directly affects the task signature")
@click.option("--project", help="Name of this Databand project")
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
    help="Associate this run with the scheduled job which will be created if needed",
)
@click.option(
    "--scheduled-date",
    "-sd",
    help="Should be used only when scheduled-job-name is set",
    type=TZAwareDateTime(),
)
@click.option("--interactive", is_flag=True, help="Run submission in blocking mode")
@click.option(
    "--submit-driver",
    "submit_driver",
    flag_value=1,
    help="Run driver at a remote engine",
)
@click.option(
    "--local-driver", "submit_driver", flag_value=0, help="Run the driver locally"
)
@click.option(
    "--submit-tasks",
    "submit_tasks",
    flag_value=1,
    help="Submit tasks from driver to a remote engine",
)
@click.option(
    "--no-submit-tasks",
    "submit_tasks",
    flag_value=0,
    help="Disable task submission, run tasks in the driver process.",
)
@click.option(
    "--disable-web-tracker", help="Disable web tracking", is_flag=True,
)
@click.option("--interactive", is_flag=True, help="Run submission in blocking mode")
@click.option(
    "--open-web-tab", help="Open the tracker URL in the Databand UI", is_flag=True
)
@click.option(
    "--docker-build-tag",
    help="Define custom docker image tag for docker image that will be built",
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
    _extend,
    verbose,
    print_task_band,
    describe,
    env,
    parallel,
    conf_file,
    task_version,
    project,
    name,
    description,
    run_driver,
    alternative_task_name,
    scheduled_job_name,
    scheduled_date,
    interactive,
    submit_driver,
    submit_tasks,
    disable_web_tracker,
    open_web_tab,
    docker_build_tag,
):
    """
    Run a task or a DAG

    To see tasks use `dbnd show-tasks` (tab completion is available).
    """

    from dbnd._core.context.databand_context import new_dbnd_context, DatabandContext
    from dbnd._core.utils.structures import combine_mappings
    from dbnd import config

    task_name = task
    # --verbose, --describe, --env, --parallel, --conf-file and --project
    # we filter out false flags since otherwise they will always override the config with their falseness
    main_switches = dict(
        databand=filter_dict_remove_false_values(
            dict(
                verbose=verbose > 0,
                task_band=print_task_band,
                describe=describe,
                env=env,
                conf_file=conf_file,
                project=project,
            )
        ),
        run=filter_dict_remove_false_values(
            dict(
                name=name,
                parallel=parallel,
                description=description,
                is_archived=describe,
            )
        ),
    )

    if submit_driver is not None:
        main_switches["run"]["submit_driver"] = bool(submit_driver)
    if submit_tasks is not None:
        main_switches["run"]["submit_tasks"] = bool(submit_tasks)
    if disable_web_tracker:
        main_switches.setdefault("core", {})["tracker_api"] = "disabled"

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
    if _extend:
        cmd_line_config.update(
            _parse_cli(_extend, source="--extend-config", extend=True,)
        )

    if _overrides:
        cmd_line_config.update(
            _parse_cli(
                _overrides,
                source="--set-override",
                priority=ConfigValuePriority.OVERRIDE,
            )
        )
    if interactive:
        cmd_line_config.update(
            _parse_cli([{"run.interactive": True}], source="--interactive")
        )
    if verbose > 1:
        cmd_line_config.update(
            _parse_cli([{"task_build.verbose": True}], source="-v -v")
        )

    if cmd_line_config:
        config.set_values(cmd_line_config, source="cmdline")
    if verbose:
        logger.info("CLI config: \n%s", pformat_config_store_as_table(cmd_line_config))

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

    with tracking_mode_context(tracking=False), new_dbnd_context(
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
        if task_cls is not None and root_task_config:
            # adding root task to configuration
            config.set_values(
                {task_cls.task_definition.task_config_section: root_task_config},
                source="--set-root",
            )

        if is_help or not task_name:
            print_help(ctx, task_cls)
            return

        if open_web_tab:
            config.set_values({"run": {"open_web_tracker_in_browser": "True"}})

        if docker_build_tag:
            config.set_values({"kubernetes": {"docker_build_tag": docker_build_tag}})

        if context.settings.system.describe:
            # we want to print describe without triggering real run
            logger.info("Building main task '%s'", task_name)
            root_task = get_task_registry().build_dbnd_task(task_name)
            root_task.ctrl.describe_dag.describe_dag()
            # currently there is bug with the click version we have when using python 2
            # so we don't use the click.echo function
            # https://github.com/pallets/click/issues/564
            print("Task %s has been described!" % task_name)
            return root_task

        return context.dbnd_run_task(
            task_or_task_name=task_name,
            run_uid=run_driver,
            scheduled_run_info=scheduled_run_info,
            project=project,
        )


def is_running_in_direct_db_mode(ctx):
    return "api" in ctx.settings.core.tracker and ctx.settings.core.tracker_api == "db"


def print_help(ctx, task_cls):
    from dbnd._core.parameter.parameter_definition import _ParameterKind

    formatter = ctx.make_formatter()
    run.format_help(ctx, formatter)
    if task_cls:
        dl = []
        for (param_name, param_obj) in task_cls.task_definition.task_param_defs.items():
            if param_obj.system or param_obj.kind == _ParameterKind.task_output:
                continue

            param_help = _help(param_obj.description)
            dl.append(("-r " + param_name, param_help))

        with formatter.section("Task"):
            formatter.write_dl(dl)
    click.echo(formatter.getvalue().rstrip("\n"), color=ctx.color)


def _parse_cli(configs, source, priority=None, extend=False):
    """
    Parse every item in configs , joining them into one big ConfigStore
    """
    config_values_list = [
        parse_and_build_config_store(
            config_values=c,
            source=source,
            priority=priority,
            auto_section_parse=True,
            extend=extend,
        )
        for c in configs
    ]
    return functools.reduce((lambda x, y: x.update(y)), config_values_list)
