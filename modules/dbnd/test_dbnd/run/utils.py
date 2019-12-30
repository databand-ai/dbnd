import logging
import os
import subprocess

from pytest import fixture

from dbnd import dbnd_run_cmd
from test_dbnd.factories import TTask


class DbndCmdTest(object):
    @fixture(autouse=True)
    def set_tmpdir(self, tmpdir):
        self.tmpdir = tmpdir

    def dbnd_run_task_with_output(
        self, run_args, task=TTask, output_parameter=TTask.t_output, call_f=dbnd_run_cmd
    ):
        local_file = str(self.tmpdir.join("output_file.txt"))
        run_args = [
            TTask.task_definition.full_task_family,
            "--set",
            "TTask.t_output=%s" % local_file,
        ] + run_args
        logging.info("Running command:%s", subprocess.list2cmdline(run_args))

        dbnd_run_cmd(run_args)
        assert os.path.exists(local_file), (
            "Output file %s wasn't created by task!" % local_file
        )
