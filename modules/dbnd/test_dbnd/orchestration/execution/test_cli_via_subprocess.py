from __future__ import absolute_import

import logging
import os
import sys

import targets

from dbnd._core.utils.basics.path_utils import relative_path
from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run
from dbnd.testing.helpers_pytest import skip_on_windows
from dbnd_test_scenarios.test_common.task import factories
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


def run_dbnd_subprocess_test(*args, **kwargs):
    logger.error(
        "  cwd='%s'," " sys.path=\n\t%s" "\nPYTHONPATH=%s",
        os.getcwd(),
        "\n\t".join(sys.path),
        os.environ.get("PYTHONPATH"),
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = ":".join(
        [
            env.get("PYTHONPATH", ""),
            relative_path(relative_path(relative_path(__file__))),
        ]  # we add current project so we can import test_dbnd
    )
    return run_dbnd_subprocess__dbnd_run(*args, module=factories, env=env, **kwargs)


class TLocalTaskDef(TTask):
    pass


@skip_on_windows
class TestSanityInvokeOverCmdline(object):
    def test_dbnd_run(self, tmpdir):
        t = targets.target(tmpdir.join("task_output"))
        args = [TTask.get_task_family(), "-r", "t_param=10", "-r", "t_output=" + t.path]
        run_dbnd_subprocess_test(args)
        assert t.exists()

    def test_task_name_with_package(self):
        # this is real command line , so the Task will not be found
        # if we don't import tests.tasks.test_cmdline_real.TLocalTaskDef
        run_dbnd_subprocess_test([TLocalTaskDef.task_definition.full_task_family])


@skip_on_windows
class TestHelpAndMisspelledOverCmdline(object):
    def test_dbnd_help(self):
        stdout = run_dbnd_subprocess_test([TTask.get_task_family(), "--help"])
        assert "-r", "t_param" in stdout

    def test_bin_mentions_misspelled_task(self):
        """
        Test that the error message is informative when a task is misspelled.

        In particular it should say that the task is misspelled and not that
        the local parameters do not exist.
        """
        stdout = run_dbnd_subprocess_test(["dbnd_sanity_che", "-r", "x=5"], retcode=5)
        print(stdout)
        assert "dbnd_sanity_check" in stdout
        assert "-r x" in stdout

    def test_no_parameters(self):
        stdout = run_dbnd_subprocess_test([], retcode=2)
        assert "Usage:" in stdout

    def test_describe_verbose(self):
        args = ["FooBaseTask", "-r", "t_param=hello", "--verbose", "--describe"]
        run_dbnd_subprocess_test(args)

    def test_describe_simple(self):
        args = [
            "-m",
            "test_dbnd.scenarios.pipelines.pipe_4tasks",
            "scenario_4_tasks.MainPipeline",
        ]
        run_dbnd_subprocess_test(args)
