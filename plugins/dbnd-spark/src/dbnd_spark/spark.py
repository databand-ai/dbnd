import logging
import subprocess
import typing

from typing import Union

from dbnd import output, parameter
from dbnd._core.commands import get_spark_session
from dbnd._core.configuration.config_path import from_task_env
from dbnd._core.constants import TaskType
from dbnd._core.current import get_databand_run
from dbnd._core.decorator.decorated_task import _DecoratedTask
from dbnd._core.decorator.func_task_decorator import _task_decorator
from dbnd._core.errors import DatabandBuildError, DatabandConfigError
from dbnd._core.task.config import Config
from dbnd._core.task.task import Task
from dbnd._core.utils.project.project_fs import databand_lib_path
from dbnd._core.utils.structures import list_of_strings
from dbnd_spark.spark_config import SparkConfig


if typing.TYPE_CHECKING:
    from dbnd_aws.emr.emr_config import EmrConfig

logger = logging.getLogger(__name__)


class _BaseSparkTask(Task):
    _conf__task_type_name = TaskType.spark

    # will get it from env
    spark_config = parameter(config_path=from_task_env("spark_config"))[SparkConfig]
    spark_engine = parameter(config_path=from_task_env("spark_engine"))[
        Config
    ]  # type: Union[EmrConfig]

    python_script = None
    main_class = None

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


class _InlineSparkTask(_BaseSparkTask, _DecoratedTask):
    _conf__require_run_dump_file = True

    def application_args(self):
        # TODO: move it to _task_submit, provide application_args from _task_submit
        return [
            "execute",
            "--disable-tracking-api",
            "--dbnd-run",
            self._get_spark_ctrl().sync(
                self._get_spark_ctrl().spark_driver_dump_file()
            ),
            "task",
            "--task-id",
            self.task_id,
        ]

    def _task_submit(self):
        dr = get_databand_run()
        if not dr.driver_dump.exists():
            raise DatabandConfigError(
                "Please configure your cloud to always_save_pipeline=True, we need pickle task first"
            )

        if self._get_spark_ctrl().should_keep_local_pickle():
            self._get_spark_ctrl().download_to_local()

        return self._get_spark_ctrl().run_pyspark(
            pyspark_script=databand_lib_path("_core", "cli", "main.py")
        )

    def _task_run(self):
        super(_InlineSparkTask, self)._task_run()
        self._get_spark_ctrl().stop_spark_session(get_spark_session())

    def run(self):
        # we actually are going to run run function via our python script
        self._invoke_func()


def spark_task(*args, **kwargs):
    kwargs.setdefault("_task_type", _InlineSparkTask)
    kwargs.setdefault("_task_default_result", parameter.output.pickle[object])
    return _task_decorator(*args, **kwargs)
