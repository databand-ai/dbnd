from __future__ import absolute_import

import logging
import subprocess
import typing

from typing import Dict, List

from dbnd import parameter
from dbnd._core.configuration.config_path import from_task_env
from dbnd._core.decorator.dbnd_func_proxy import _task_decorator
from dbnd._core.decorator.decorated_task import _DecoratedTask
from dbnd._core.errors import DatabandConfigError, friendly_error
from dbnd._core.settings.engine import EngineConfig
from dbnd._core.task_ctrl.task_repr import _parameter_value_to_argparse_str
from dbnd.tasks import Config, Task
from dbnd_gcp.apache_beam import ApacheBeamJobCtrl


if typing.TYPE_CHECKING:
    from dbnd_gcp.dataflow.dataflow_config import DataflowConfig

logger = logging.getLogger(__name__)


class ApacheBeamConfig(Config):
    """Apache Beam (-s [TASK].spark.[PARAM]=[VAL] for specific tasks)"""

    # we don't want spark class to inherit from this one, as it should has Config behaviour
    _conf__task_family = "beam"

    jar = parameter.value(None, description="Main application jar")[str]

    verbose = parameter.value(
        False,
        description="Whether to pass the verbose flag to spark-submit process for debugging",
    )

    options = parameter(empty_default=True)[Dict[str, str]]


class LocalBeamEngineConfig(EngineConfig):
    def get_beam_ctrl(self, task_run):
        from dbnd_gcp.apache_beam.local_apache_beam import LocalApacheBeamJobCtrl

        return LocalApacheBeamJobCtrl(task_run)


class _BeamTask(Task):
    beam_config = parameter(config_path=from_task_env("beam_config"))[ApacheBeamConfig]
    beam_engine = parameter(config_path=from_task_env("beam_engine"))[
        EngineConfig
    ]  # type: DataflowConfig

    def get_beam_task_options(self):
        """
        'Arguments for the application being submitted'
        :return: list
        """
        return {}

    def _get_job_ctrl(self):
        # type:(...)->ApacheBeamJobCtrl
        return self.beam_engine.get_beam_ctrl(self.current_task_run)


class ApacheBeamJavaTask(_BeamTask):
    main_class = parameter.none(
        description="The entry point for your application", system=True
    )[str]

    def _task_submit(self):
        if not self.beam.main_jar:
            raise DatabandConfigError("main_jar is not configured for %s" % self)

        return self._get_job_ctrl().run_cmd_java(
            jar=self.beam.jar, main_class=self.main_class
        )


class ApacheBeamPythonTask(_BeamTask):
    py_file = parameter.help("The application that submitted as  *.py file").none[str]
    py_options = parameter(empty_default=True)[List[str]]

    def _task_submit(self):
        return self._get_job_ctrl().run_cmd_python(
            py_file=self.py_file, py_options=self.py_options
        )


class _ApacheBeamInlineTask(_BeamTask, _DecoratedTask):
    _conf__require_run_dump_file = True

    dataflow_build_pipeline = parameter(
        system=True, description="Build Pipeline object if not set"
    ).value(True)

    dataflow_wait_until_finish = parameter(
        system=True, description="Automatically wait for pipeline run finish"
    ).value(True)

    dataflow_submit_dbnd_info = parameter(
        system=True, description="Add databand data to PipelineOptions"
    ).value(True)

    dataflow_pipeline = parameter(system=True, description="Dataflow Pipeline").value(
        None
    )[object]

    def _task_run(self):
        super(_ApacheBeamInlineTask, self)._task_run()

    def _task_options(self, python_pipeline_options):
        # We use the save_main_session option because one or more DoFn's in this
        # workflow rely on global context (e.g., a module imported at module level).
        from apache_beam.options.pipeline_options import PipelineOptions

        user_params = self._params.get_param_values(user_only=True)

        class TaskOptions(PipelineOptions):
            @classmethod
            def _add_argparse_args(cls, parser):
                for dbnd_key in [
                    "databand_url",
                    "task_id",
                    "task_function",
                    "task_vcs",
                ]:
                    parser.add_argument(
                        "--dbnd-%s" % (dbnd_key.replace("_", "-")),
                        help="Databand %s" % dbnd_key,
                        default=None,
                        dest="dbnd_%s" % dbnd_key,
                    )
                for param_obj, param_name in user_params:
                    action = (
                        "store_true" if param_obj.value_type.type is bool else "store"
                    )
                    parser.add_argument(
                        ["--" + param_name.lower().replace("_", "-")],
                        help=param_obj.description,
                        default=None,
                        action=action,
                        dest=param_name,
                    )

        for p, p_value in user_params:
            argparse_str = _parameter_value_to_argparse_str(p, p_value)
            python_pipeline_options.extend(argparse_str)

        python_pipeline_options.extend(
            [
                "--dbnd-tracker-url",
                self.ctrl.task_run.task_tracker_url,
                "--dbnd-task-id",
                self.task_id,
                "--dbnd-task-function",
                self.task_definition.full_task_family,
                "--dbnd-task-vcs",
                self.ctrl.task_run.run.source_version,
            ]
        )

        logger.info(
            "Creating beam pipeline options: %s",
            subprocess.list2cmdline(python_pipeline_options),
        )
        pipeline_options = TaskOptions(python_pipeline_options)
        return pipeline_options

    def run(self):
        # We use the save_main_session option because one or more DoFn's in this
        # workflow rely on global context (e.g., a module imported at module level).

        if self.dataflow_pipeline is None and self.dataflow_build_pipeline:
            self.dataflow_pipeline = self.build_pipeline()

        self._invoke_func()

        if self.dataflow_wait_until_finish:
            if self.dataflow_pipeline is None:
                raise friendly_error.tools.dataflow_pipeline_not_set(self)
            result = self.dataflow_pipeline.run()
            result.wait_until_finish()

    def build_pipeline(self, extra_pipeline_options=None):
        python_pipeline_options = self._get_job_ctrl().get_python_options()
        if extra_pipeline_options:
            python_pipeline_options.extend(extra_pipeline_options)

        if self.dataflow_submit_dbnd_info:
            pipeline_options = self._task_options(python_pipeline_options)
        else:
            logger.info(
                "Creating beam pipeline options: %s",
                subprocess.list2cmdline(python_pipeline_options),
            )
            from apache_beam.options.pipeline_options import PipelineOptions

            pipeline_options = PipelineOptions(python_pipeline_options)
        import apache_beam

        return apache_beam.Pipeline(options=pipeline_options)


def beam_task(*args, **kwargs):
    kwargs.setdefault("_task_type", _ApacheBeamInlineTask)
    kwargs.setdefault("_task_default_result", parameter.output.pickle[object])
    return _task_decorator(*args, **kwargs)
