import copy
import typing

import six

from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.utils.json_utils import dumps_canonical
from dbnd._core.utils.string_utils import camel_to_snake


if typing.TYPE_CHECKING:
    from dbnd_gcp.apache_beam.apache_beam_task import ApacheBeamConfig


class ApacheBeamJobCtrl(TaskRunCtrl):
    @property
    def config(self):
        # type: (ApacheBeamJobCtrl) -> ApacheBeamConfig
        return self.task.beam_config

    def _get_base_options(self):
        """
        use to override with internal implementation
        :return:
        """
        options = copy.copy(self.config.options)
        options["jobName"] = self.job.job_id.replace("_", "-")
        return options

    def _get_options(self):
        """
        access all options from here
        :return:
        """
        options = self._get_base_options()
        options.update(self.task.get_beam_task_options())
        return options

    def _build_cmd_line(self, options):
        command = []
        for attr, value in six.iteritems(options):
            if value is None:
                continue
            command.append("--" + attr + "=" + str(value))
        return command

    def run_cmd_java(self, jar, main_class):
        options = self._get_options()

        if "labels" in options:
            options["labels"] = dumps_canonical(options["labels"])

        if main_class:
            cmd = ["java", "-cp", jar, main_class]
        else:
            cmd = ["java", "-jar", jar]

        for attr, value in six.iteritems(options):
            if value is None:
                continue
            cmd.append("--" + attr + "=" + str(value))
            # run job
        self._run_cmd(cmd)

    def get_python_options(self):
        cmd = []
        options = self._get_options()
        for attr, value in six.iteritems(options):
            if value is None:
                continue
            if attr == "labels":
                labels = [
                    "--labels={}={}".format(key, value)
                    for key, value in six.iteritems(value)
                ]
                cmd.extend(labels)
            else:
                cmd.append("--" + camel_to_snake(attr) + "=" + str(value))
        return cmd

    def run_cmd_python(self, py_file, py_options):
        cmd = ["python"] + py_options + [py_file]
        cmd.extend(self.get_python_options())
        # run job
        self._run_cmd(cmd)

    def _run_cmd(self, cmd):
        pass
