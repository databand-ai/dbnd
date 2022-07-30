# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing
import uuid

from typing import List, Optional, Union
from uuid import UUID

from dbnd._core.configuration.config_readers import read_from_config_files
from dbnd._core.configuration.dbnd_config import DbndConfig, config
from dbnd._core.configuration.environ_config import is_unit_test_mode
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.log import dbnd_log
from dbnd._core.plugin.dbnd_plugins import pm
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.settings import DatabandSystemConfig, OutputConfig, RunInfoConfig
from dbnd._core.task.task import Task
from dbnd._core.task_build.task_instance_cache import TaskInstanceCache
from dbnd._core.task_executor.run_executor import RunExecutor
from dbnd._core.tracking.schemas.tracking_info_run import ScheduledRunInfo
from dbnd._core.utils import seven
from dbnd._core.utils.basics.load_python_module import load_python_module, run_user_func
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd._core.utils.task_utils import get_project_name_safe, get_task_name_safe
from dbnd._core.utils.timezone import utcnow
from targets.target_config import FileFormat


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun

if typing.TYPE_CHECKING:
    from typing import ContextManager

logger = logging.getLogger(__name__)


class DatabandContext(SingletonContext):
    """
    Helper for parsing command line arguments and configuration and used as part of the
    context when instantiating task objects.
    Holds state of all "config" providers, like cmd line and configuration files

    This object is managed via SingletonContext, with on_enter and on_exit functions

    When Object is built it accesses settings/config via this object
    So we can't build object till we don't get to on_enter function.
    If we don't have root  task - that means we run without command line - programmatic invocation,
    we'll create new Dag and run everything under its context

    """

    _current_context_id = 0

    # controls load of orm dags by versioned airflow
    # we will set it to true when we run airflow original commands
    def __init__(self, name="global"):
        """
        Initialize cmd line args
        """
        super(DatabandContext, self).__init__()
        # double checking on bootstrap, as we can run from all kind of locations
        # usually we should be bootstraped already as we run from cli.
        dbnd_bootstrap()

        self.name = name

        self.current_context_uid = "%s_%s" % (
            utcnow().strftime("%Y%m%d_%H%M%S"),
            str(uuid.uuid1())[:8],
        )

        self.config = config

        # we are running from python notebook, let start to print to stdout
        if self.name == "interactive" or is_unit_test_mode():
            self.config.set("log", "stream_stdout", "True", source="log")

        self.task_instance_cache = TaskInstanceCache()
        self.user_code_detector = UserCodeDetector.build_code_detector()

        # will set up in __enter__
        # we can't initialize settings without having self defined as context
        # we assign real object only in _on_enter, however it's great for auto completion
        from dbnd._core.settings import DatabandSettings

        self.settings = None  # type: DatabandSettings
        self._is_initialized = False

        self._tracking_store = None

    def _on_enter(self):
        pm.hook.dbnd_on_pre_init_context(ctx=self)
        run_user_func(config.get("core", "user_pre_init"))
        # if we are deserialized - we don't need to run this code again.
        if not self._is_initialized:
            # will be called from singleton context manager
            self.system_settings = DatabandSystemConfig()
            if self.system_settings.conf:
                self.config.set_values(
                    self.system_settings.conf, source="[databand]conf"
                )
            if self.system_settings.conf_file:
                conf_file = read_from_config_files(self.system_settings.conf_file)
                self.config.set_values(conf_file, source="[databand]conf")

            from dbnd._core.settings import DatabandSettings

            self.settings = DatabandSettings(databand_context=self)
            self.env = self.settings.get_env_config(self.system_settings.env)
            self.config.set_values(
                config_values={"task": {"task_env": self.system_settings.env}},
                source="context",
            )

            pm.hook.dbnd_on_new_context(ctx=self)

            # RUN USER SETUP FUNCTIONS
            _run_user_func(
                self.settings.core.__class__.user_driver_init,
                self.settings.core.user_driver_init,
            )

            self.task_run_env = RunInfoConfig().build_task_run_info()
            self._is_initialized = True
        else:
            # we get here if we are running at sub process that recreates the Context
            pm.hook.dbnd_on_existing_context(ctx=self)

        if self.system_settings.verbose:
            # propagate value of system_settings
            # we need to do it also when we are loaded from pickle.
            self._original_verbose = dbnd_log.is_verbose()
            dbnd_log.set_verbose()

        # we do it every time we go into databand_config
        self.configure_targets()
        self.settings.log.configure_dbnd_logging()

        _run_user_func(
            self.settings.core.__class__.user_init, self.settings.core.user_init
        )
        pm.hook.dbnd_post_enter_context(ctx=self)

    @property
    def tracking_store(self):
        if self._tracking_store is None:
            self._tracking_store = self.settings.core.build_tracking_store(self)
        return self._tracking_store

    @property
    @cached()
    def databand_api_client(self):
        return self.settings.core.build_databand_api_client()

    def configure_targets(self):
        output_config = self.settings.output  # type: OutputConfig
        if output_config.hdf_format == "table":
            import pandas as pd

            from targets.marshalling import MARSHALERS
            from targets.marshalling.pandas import DataFrameToHdf5Table

            MARSHALERS[pd.DataFrame][FileFormat.hdf5] = DataFrameToHdf5Table()

    def _on_exit(self):
        if self._tracking_store:
            self.tracking_store.flush()

        if self.system_settings.verbose:
            dbnd_log.set_verbose(self._original_verbose)

        pm.hook.dbnd_on_exit_context(ctx=self)

    def is_interactive(self):
        return self.name == "interactive"

    def __repr__(self):
        return "%s(name='%s')" % (self.__class__.__name__, self.name)

    def dbnd_run_task(
        self,
        task_or_task_name,  # type: Union[Task, str]
        job_name=None,  # type: Optional[str]
        force_task_name=None,  # type: Optional[str]
        project=None,  # type: Optional[str]
        run_uid=None,  # type: Optional[UUID]
        existing_run=None,  # type: Optional[bool]
        scheduled_run_info=None,  # type: Optional[ScheduledRunInfo]
        send_heartbeat=True,  # type: bool
    ):  # type: (...) -> DatabandRun
        """
        This is the main entry point to run task in "dbnd orchestration" mode
        called from `dbnd run`
        we create a new Run + RunExecutor and trigger the execution

        :param task_or_task_name task name to run or already built task object
        :param force_task_name
        :param project Project name for the run
        :return DatabandRun
        """
        job_name = get_task_name_safe(job_name or task_or_task_name)
        project_name = self._get_project_name(project, task_or_task_name)

        with new_databand_run(
            context=self,
            job_name=job_name,
            run_uid=run_uid,
            existing_run=existing_run,
            scheduled_run_info=scheduled_run_info,
            is_orchestration=True,
            project_name=project_name,
        ) as run:  # type: DatabandRun
            # this is the main entry point to run some task in "orchestration" mode
            run.run_executor = RunExecutor(
                run=run,
                root_task_or_task_name=task_or_task_name,
                force_task_name=force_task_name,
                send_heartbeat=send_heartbeat,
            )
            run.run_executor.run_execute()
            return run

    def _get_project_name(self, project, task_or_task_name):
        # type: (Optional[str], Union[Task, str]) -> str
        return get_project_name_safe(
            project
            or self.config.get("tracking", "project")
            or self.settings.tracking.project,
            task_or_task_name,
        )

    def __deepcopy__(self, memo):
        # create a copy with self.linked_to *not copied*, just referenced.
        return self

    def set_current(self, name, description=None):
        self.settings.run.name = name
        self.settings.run.description = description


def _run_user_func(param, value):
    if not value:
        return
    return run_user_func(value)


def load_user_modules(dbnd_config, modules=None):
    # type: (DbndConfig, List[str]) -> None
    # loading user modules
    module_from_config = dbnd_config.get("databand", "module")
    if module_from_config:
        load_python_module(module_from_config, "config file (see [databand].module)")
    if modules:
        for m in modules:
            load_python_module(m, "--module")


@seven.contextlib.contextmanager
def new_dbnd_context(conf=None, name=None, **kwargs):
    # type: (...) -> ContextManager[DatabandContext]
    """Creates a new DatabandContext."""
    with config(config_values=conf, source="inplace"):
        with DatabandContext.new_context(
            name=name, allow_override=True, **kwargs
        ) as dc:
            yield dc
