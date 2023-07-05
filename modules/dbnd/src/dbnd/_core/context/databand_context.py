# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing
import uuid

from typing import Optional, Union
from uuid import UUID

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.context.use_dbnd_run import (
    assert_dbnd_orchestration_enabled,
    is_orchestration_mode,
)
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.log import dbnd_log
from dbnd._core.settings import DatabandSystemConfig, RunInfoConfig
from dbnd._core.task_build.task_instance_cache import TaskInstanceCache
from dbnd._core.tracking.schemas.tracking_info_run import ScheduledRunInfo
from dbnd._core.utils import seven
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd._core.utils.timezone import utcnow


if typing.TYPE_CHECKING:
    from typing import ContextManager

    from dbnd._core.run.databand_run import DatabandRun
    from dbnd_run.run_settings import RunSettings

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
        self.name = name

        self.current_context_uid = "%s_%s" % (
            utcnow().strftime("%Y%m%d_%H%M%S"),
            str(uuid.uuid1())[:8],
        )

        self.config = config

        self.task_instance_cache = TaskInstanceCache()
        self.user_code_detector = UserCodeDetector.build_code_detector()

        # will set up in __enter__
        # we can't initialize settings without having self defined as context
        # we assign real object only in _on_enter, however it's great for auto completion
        from dbnd._core.settings import DatabandSettings

        self.settings: DatabandSettings = None

        self.run_settings: "RunSettings" = None
        self.system_settings: DatabandSystemConfig = None

        self._is_initialized = False

        self._tracking_store = None

    def _on_enter(self):
        # if we are deserialized - we don't need to run this code again.
        if not self._is_initialized:
            # will be called from singleton context manager
            # we need DatabandContext to be available to create all these objects
            self.system_settings = DatabandSystemConfig()
            self.system_settings.update_config_from_user_inputs(self.config)

            from dbnd._core.settings import DatabandSettings

            self.settings = DatabandSettings(databand_context=self)

            if is_orchestration_mode():
                from dbnd_run.run_settings import RunSettings

                self.run_settings = RunSettings(databand_context=self)

            self.task_run_env = RunInfoConfig().build_task_run_info()
            self._is_initialized = True

        if self.system_settings.verbose:
            # propagate value of system_settings
            # we need to do it also when we are loaded from pickle.
            self._original_verbose = dbnd_log.is_verbose()
            dbnd_log.set_verbose()

    @property
    def tracking_store(self):
        if self._tracking_store is None:
            self._tracking_store = self.settings.core.build_tracking_store(self)
        return self._tracking_store

    @property
    @cached()
    def databand_api_client(self):
        return self.settings.core.build_databand_api_client()

    def _on_exit(self):
        if self._tracking_store:
            self.tracking_store.flush()

        if self.system_settings and self.system_settings.verbose:
            dbnd_log.set_verbose(self._original_verbose)

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
        Deprecated in favor of dbnd_run_task
        """

        assert_dbnd_orchestration_enabled()

        from dbnd_run.run_executor.run_executor import dbnd_run_task

        run_executor = dbnd_run_task(
            task_or_task_name=task_or_task_name,
            context=self,
            job_name=job_name,
            force_task_name=force_task_name,
            project=project,
            run_uid=run_uid,
            existing_run=existing_run,
            scheduled_run_info=scheduled_run_info,
            send_heartbeat=send_heartbeat,
        )
        return run_executor.run

    def __deepcopy__(self, memo):
        # create a copy with self.linked_to *not copied*, just referenced.
        return self


@seven.contextlib.contextmanager
def new_dbnd_context(conf=None, name=None, **kwargs):
    # type: (...) -> ContextManager[DatabandContext]
    """Creates a new DatabandContext."""
    with config(config_values=conf, source="inplace"):
        with DatabandContext.new_context(
            name=name, allow_override=True, **kwargs
        ) as dc:
            yield dc
