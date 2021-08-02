import typing

from dbnd._vendor import pluggy


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun


hookspec = pluggy.HookspecMarker("dbnd")


@hookspec
def dbnd_setup_plugin():
    """ Called right after plugin is imported  """
    pass


@hookspec
def dbnd_get_commands():
    """ Called from main cli entry point """
    pass


@hookspec
def dbnd_on_pre_init_context(ctx):
    """ Called from DatabandContext before entering a new context """
    pass


@hookspec
def dbnd_on_new_context(ctx):
    """ Called from DatabandContext when entering a new context """
    pass


@hookspec
def dbnd_on_existing_context(ctx):
    """ Called from DatabandContext when entering an existing context """
    pass


@hookspec
def dbnd_post_enter_context(ctx):
    """ Called from DatabandContext after enter initialization steps """
    pass


@hookspec
def dbnd_on_exit_context(ctx):
    """ Called from DatabandContext when exiting """
    pass


@hookspec
def dbnd_setup_unittest():
    """ Called when running in test mode  """
    pass


@hookspec
def dbnd_task_run_context(task_run):
    # type: (TaskRun)-> Context
    """ Using this context when running task_run  """
    pass


@hookspec
def dbnd_build_project_docker(docker_engine, docker_build_task):
    """ Runs on docker build """
    pass
