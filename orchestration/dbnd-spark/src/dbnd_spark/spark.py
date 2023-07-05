# © Copyright Databand.ai, an IBM Company 2022

import logging
import subprocess
import typing

from typing import Union

import more_itertools

from dbnd._core.constants import TaskType
from dbnd._core.errors import DatabandBuildError, DatabandConfigError
from dbnd._core.errors.friendly_error.task_build import incomplete_output_found_for_task
from dbnd._core.parameter.parameter_builder import output, parameter
from dbnd._core.task_build.dbnd_decorator import build_task_decorator
from dbnd._core.utils.project.project_fs import databand_lib_path
from dbnd._core.utils.structures import list_of_strings
from dbnd._core.utils.traversing import flatten
from dbnd_run.cli.cmd_execute import get_dbnd_version, get_python_version
from dbnd_run.task.decorated_callable_task import _DecoratedCallableTask
from dbnd_run.task.task import Task
from dbnd_run.tasks.py_distribution.fat_wheel_tasks import ProjectWheelFile
from dbnd_spark.local.local_spark_config import SparkLocalEngineConfig
from dbnd_spark.spark_config import SparkConfig, SparkEngineConfig
from dbnd_spark.spark_session import get_spark_session
from targets import DirTarget
from targets.types import PathStr


if typing.TYPE_CHECKING:
    from dbnd_aws.emr.emr_config import EmrConfig

logger = logging.getLogger(__name__)


class _BaseSparkTask(Task):
    _conf__task_type_name = TaskType.spark

    # will get it from env
    spark_config = parameter(from_task_env_config=True)[SparkConfig]
    spark_engine = parameter(from_task_env_config=True)[
        SparkEngineConfig
    ]  # type: Union[EmrConfig]

    python_script = None
    main_class = None

    spark_project_wheel_file = parameter(
        empty_default=True, default=None, system=True, significant=False
    )[PathStr]

    def _complete(self):
        dir_outputs = self._get_dir_outputs()
        incomplete_outputs, complete_outputs = map(
            list, more_itertools.partition(lambda target: target.exists(), dir_outputs)
        )
        if incomplete_outputs:
            exc = incomplete_output_found_for_task(
                self.task_name, complete_outputs, incomplete_outputs
            )
            if self.task_env.settings.run.validate_task_outputs_on_build:
                raise exc

            logger.warning(str(exc))
            return False

        return super(_BaseSparkTask, self)._complete()

    def _get_dir_outputs(self):
        # We address targets as directories in spark task because spark creates success flags in directory.
        # All FileTargets that run on spark actually create directories!
        dir_outputs = [
            DirTarget(target.path + "/", target.fs, config=target.config)
            for target in flatten(self.task_outputs)
            if target.fs.isdir(target.path) and not target.config.overwrite_target
        ]  # All outputs that are directories and are NOT overwrite targets
        return dir_outputs

    def band(self):
        result = super(_BaseSparkTask, self).band()

        # We use fat wheel task in the band function because it is executed when we are still in the build stage
        if self.spark_config.include_user_project:
            fat_wheel_task = ProjectWheelFile.build_project_wheel_file_task()
            self.spark_project_wheel_file = fat_wheel_task.wheel_file

        if self.spark_engine.disable_task_band:
            logger.debug("Task band is disabled due to disable_task_band flag")
            self.task_band = None

        return result

    def get_py_files(self):
        py_files = self.spark_config.py_files.copy()
        if self.spark_project_wheel_file:
            py_files.append(str(self.spark_project_wheel_file))
        return py_files

    def application_args(self):
        """
        'Arguments for the application being submitted'
        :return: list
        """
        return []

    def _get_spark_ctrl(self):
        # type: ()-> SparkCtrl
        return self.spark_engine.get_spark_ctrl(self.current_task_run)

    def _task_banner(self, banner, verbosity):
        b = banner

        b.new_section()
        try:
            spark_command_line = subprocess.list2cmdline(
                list_of_strings(self.application_args())
            )
            b.column("SPARK CMD LINE", spark_command_line)
        except Exception:
            logger.exception("Failed to get spark command line from %s" % self)

    def get_root(self):
        return self.spark_engine.root or super(_BaseSparkTask, self).get_root()


spark_output = output.folder


class SparkTask(_BaseSparkTask):
    main_class = parameter(
        default=None, description="The entry point for your application", system=True
    )[str]

    def _task_submit(self):
        if not self.spark_config.main_jar:
            raise DatabandConfigError("main_jar is not configured for %s" % self)
        return self._get_spark_ctrl().run_spark(main_class=self.main_class)

    def run(self):
        # we don't actually have run function except for inline
        #
        # we don't want to read params automatically
        # most likely our run function just creates a command line to run on remote compute
        raise DatabandBuildError("You should not call or override run functions")


class PySparkTask(_BaseSparkTask):
    _conf__task_type_name = TaskType.pyspark

    python_script = parameter(
        description="The application that submitted as a job *.py file"
    )[str]

    def _task_submit(self):
        return self._get_spark_ctrl().run_pyspark(pyspark_script=self.python_script)


class PySparkInlineTask(_BaseSparkTask):
    _application_args = ["defined_at_runtime"]

    def application_args(self):
        return self._application_args

    def _use_spark_context_inplace(self):
        from dbnd.providers.spark.dbnd_spark_init import has_active_spark_session

        if isinstance(self.spark_engine, SparkLocalEngineConfig):
            if (
                self.spark_engine.enable_spark_context_inplace
                and has_active_spark_session()
            ):
                return True

        if self.spark_config.use_current_spark_session and has_active_spark_session():
            return True

        return False

    @property
    def _conf__require_run_dump_file(self):
        # if we run inplace we don't want to save dataframe in case user have real Spark DataFrame as a parameter
        return not self._use_spark_context_inplace()

    def _task_submit(self):
        spark_ctrl = self._get_spark_ctrl()
        if self._use_spark_context_inplace():
            logger.info(
                "Reusing existing spark session in inline task "
                "due to spark_local.enable_spark_context_inplace"
            )
            return self._task_run()
        driver_dump = self.current_task_run.run.run_executor.driver_dump
        if not driver_dump:
            raise DatabandConfigError(
                "Please configure your cloud to always_save_pipeline=True, we need to pickle pipeline first"
            )
        self._application_args = [
            "execute",
            "--expected-dbnd-version",
            get_dbnd_version(),
            "--expected-python-version",
            get_python_version(),
            "--dbnd-run",
            spark_ctrl.sync(driver_dump),
            "task",
            "--task-id",
            self.task_id,
        ]

        if spark_ctrl.config.disable_tracking_api:
            self._application_args[1:1] = ["--disable-tracking-api"]

        return spark_ctrl.run_pyspark(
            pyspark_script=databand_lib_path("_core", "cli", "main.py")
        )

    def _task_run(self):
        super(PySparkInlineTask, self)._task_run()

        if self._get_spark_ctrl().stop_spark_session_on_finish:
            session = get_spark_session()
            logger.info("Stopping spark session: %s")
            session.stop()


class _PySparkInlineFuncTask(PySparkInlineTask, _DecoratedCallableTask):
    def run(self):
        # we actually are going to run run function via our python script
        self._invoke_func()


def spark_task(*args, **kwargs):
    kwargs.setdefault("_task_type", _PySparkInlineFuncTask)
    kwargs.setdefault("_task_default_result", parameter.output.pickle[object])
    return build_task_decorator(*args, **kwargs)
