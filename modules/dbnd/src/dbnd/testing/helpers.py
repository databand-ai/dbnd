# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import shlex
import subprocess
import sys

from subprocess import list2cmdline

from dbnd import dbnd_config
from dbnd._core.configuration.environ_config import ENV_DBND_HOME
from dbnd._core.current import dbnd_context
from dbnd._core.settings import RunConfig
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.tools.jupyter.notebook import notebook_run
from dbnd._core.utils import seven
from dbnd._core.utils.basics import fast_subprocess
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._core.utils.project.project_fs import abs_join


logger = logging.getLogger(__name__)


def get_environ_without_dbnd_and_airflow_vars():
    env = os.environ.copy()
    for key in list(env.keys()):
        if key.startswith("DBND") or key.startswith("AIRFLOW"):
            del env[key]
    return env


def run_dbnd_subprocess(args, retcode=255, blocking=True, **kwargs):
    # implement runner with https://docs.pytest.org/en/latest/capture.html
    # do not run in subprocess
    # main.main(['run', '--module', str(factories.__name__), ] + args)
    # return
    kwargs = kwargs.copy()
    cmd_args = list(map(str, args))
    env = kwargs.pop("env", os.environ).copy()

    # env['PYTHONUNBUFFERED'] = 'false'
    # env['PYTHONPATH'] = env.get('PYTHONPATH', '') + ':.:test'

    cmd_line = list2cmdline(cmd_args)

    logger.info(
        "Running at %s: %s", kwargs.get("cwd", "current dir"), cmd_line
    )  # To simplify rerunning failing tests

    if blocking:
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
    else:
        return subprocess.Popen(cmd_args, stderr=subprocess.STDOUT, env=env, **kwargs)


def run_test_notebook(notebook):
    return notebook_run(notebook)


def run_subprocess__airflow(args, retcode=255, **kwargs):
    return run_dbnd_subprocess(
        args=["airflow"] + args,
        cwd=kwargs.pop("cwd", os.environ[ENV_DBND_HOME]),
        retcode=retcode,
        **kwargs
    )


def run_dbnd_subprocess__dbnd(args, retcode=255, **kwargs):
    if isinstance(args, str):
        args = shlex.split(args, posix=not windows_compatible_mode)
    return run_dbnd_subprocess(
        args=[sys.executable, "-m", "dbnd"] + args,
        cwd=kwargs.pop("cwd", os.environ[ENV_DBND_HOME]),
        retcode=retcode,
        **kwargs
    )


def run_dbnd_subprocess__dbnd_web(args, retcode=255, **kwargs):
    if isinstance(args, str):
        args = shlex.split(args, posix=not windows_compatible_mode)
    return run_dbnd_subprocess(
        args=["dbnd-web"] + args,
        cwd=kwargs.pop("cwd", os.environ[ENV_DBND_HOME]),
        retcode=retcode,
        **kwargs
    )


def run_dbnd_subprocess__with_home(args, retcode=255, **kwargs):
    return run_dbnd_subprocess(
        args=[sys.executable] + args,
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


@seven.contextlib.contextmanager
def initialized_run(task):
    with dbnd_config({RunConfig.dry: True}):
        dbnd_context().dbnd_run_task(task_or_task_name=task)


def dbnd_module_path():
    return abs_join(__file__, "..", "..", "..", "..")


def dbnd_examples_path():
    return abs_join(dbnd_module_path(), "..", "..", "examples", "src")


def import_all_modules(src_dir, package, excluded=None):
    packagedir = os.path.join(src_dir, package)
    errors = []
    imported_modules = []

    def import_module(p):
        try:
            logger.info("Importing %s", p)
            __import__(p)
            imported_modules.append(p)
        except Exception as ex:
            errors.append(ex)
            logger.exception("Failed to import %s", p)

    excluded = excluded or set()

    for root, subdirs, files in os.walk(packagedir):
        package = os.path.relpath(root, start=src_dir).replace(os.path.sep, ".")

        if any([p in root for p in excluded]):
            continue

        if "__init__.py" not in files:
            continue

        import_module(package)

        for f in files:
            if f.endswith(".py") and not f.startswith("_"):
                import_module(package + "." + f[:-3])

    return imported_modules
