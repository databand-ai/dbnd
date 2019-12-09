import logging
import os
import shlex
import subprocess
import sys

from subprocess import list2cmdline

from dbnd._core.context.dbnd_project_env import ENV_DBND_HOME
from dbnd._core.current import dbnd_context
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.tools.jupyter.notebook import notebook_run
from dbnd._core.utils import seven
from dbnd._core.utils.basics import fast_subprocess
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._core.utils.project.project_fs import abs_join


logger = logging.getLogger(__name__)


def run_dbnd_subprocess(args, retcode=255, clean_env=False, **kwargs):
    # implement runner with https://docs.pytest.org/en/latest/capture.html
    # do not run in subprocess
    # main.main(['run', '--module', str(factories.__name__), ] + args)
    # return
    kwargs = kwargs.copy()
    cmd_args = list(map(str, args))
    env = kwargs.pop("env", os.environ).copy()

    if clean_env:
        for key in list(env.keys()):
            if key.startswith("DBND") or key.startswith("AIRFLOW"):
                del env[key]

    # env['PYTHONUNBUFFERED'] = 'false'
    # env['PYTHONPATH'] = env.get('PYTHONPATH', '') + ':.:test'

    from dbnd._core.current import get_databand_context

    env[
        "DBND__CORE__SQL_ALCHEMY_CONN"
    ] = get_databand_context().settings.core.get_sql_alchemy_conn()

    cmd_line = list2cmdline(cmd_args)

    logger.info(
        "Running at %s: %s", kwargs.get("cwd", "current dir"), cmd_line
    )  # To simplify rerunning failing tests
    try:
        output = fast_subprocess.check_output(
            cmd_args, stderr=subprocess.STDOUT, env=env, **kwargs
        )
        # we don't decode ascii //.decode("ascii")
        output = output.decode("utf-8")
        logger.info("Cmd line %s output:\n %s", cmd_line, output)
        return output
    except subprocess.CalledProcessError as ex:
        logger.error(
            "Failed to run %s :\n\n\n -= Output =-\n%s\n\n\n -= See output above =-",
            cmd_line,
            ex.output.decode("utf-8", errors="ignore"),
        )
        if ex.returncode == retcode:
            return ex.output.decode("utf-8")
        raise ex


def run_test_notebook(notebook):
    return notebook_run(notebook)


def run_dbnd_subprocess__dbnd(args, retcode=255, **kwargs):
    return run_dbnd_subprocess(
        args=[sys.executable, "-m", "dbnd"] + args,
        cwd=kwargs.pop("cwd", os.environ[ENV_DBND_HOME]),
        retcode=retcode,
        **kwargs
    )


def run_dbnd_subprocess__dbnd_run(args, module=None, retcode=255, **kwargs):
    if module:
        args = ["--module", str(module.__name__)] + args

    return run_dbnd_subprocess__dbnd(["run"] + args, retcode=retcode, **kwargs)


def build_task(root_task, **kwargs):
    from dbnd import new_dbnd_context

    with new_dbnd_context(conf={root_task: kwargs}):
        return get_task_registry().build_dbnd_task(task_name=root_task)


def init_airflow_test_config():
    if is_airflow_enabled():
        from airflow import configuration
        from airflow.configuration import TEST_CONFIG_FILE

        configuration.conf.read(TEST_CONFIG_FILE)


def run_dbnd_test_project(project_dir, args_str):
    args = shlex.split(args_str, posix=not windows_compatible_mode)

    output = run_dbnd_subprocess__dbnd(args=args, clean_env=True, cwd=project_dir)
    logger.warning("Test project at %s: '%s'", project_dir, args_str)
    return output


@seven.contextlib.contextmanager
def initialized_run(task_or_task_name):
    with new_databand_run(
        context=dbnd_context(), task_or_task_name=task_or_task_name
    ) as r:
        r._init_without_run()
        yield r


def dbnd_module_path():
    return abs_join(__file__, "..", "..", "..", "..")
