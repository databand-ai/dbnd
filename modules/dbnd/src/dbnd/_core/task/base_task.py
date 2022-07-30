# © Copyright Databand.ai, an IBM Company 2022

"""
The abstract :py:class:`_BaseTask` class.
"""

import logging

from typing import TYPE_CHECKING, Optional

from dbnd._core.constants import TaskType
from dbnd._core.current import get_databand_context
from dbnd._core.errors import friendly_error
from dbnd._core.parameter.parameter_value import Parameters
from dbnd._core.utils.basics.nothing import NOTHING


if TYPE_CHECKING:
    from dbnd._core.settings import DatabandSettings
    from dbnd._core.task_build.task_decorator import TaskDecorator
    from dbnd._core.task_build.task_definition import TaskDefinition

logger = logging.getLogger(__name__)


class _BaseTask(object):
    _current_task_creation_id = 0

    # override to change get_task_family() -> changes task_family
    _conf__task_family = None
    _conf__track_source_code = True  # module/class/function user source code tracking

    _conf__task_type_name = TaskType.python
    _conf__task_ui_color = None
    #####
    # override output path format
    _conf__base_output_path_fmt = None

    _conf__tracked = True  # track task changes with TrackingStore

    # stores call spec for the @task definition
    task_decorator = None  # type: Optional[TaskDecorator]
    task_namespace = NOTHING

    @property
    def task_family(self):
        return self.task_definition.task_passport.task_family

    @property
    def _params(self):
        return self.task_params

    def __init__(
        self,
        task_name,
        task_definition,  # type: TaskDefinition
        task_signature_obj,
        task_params,  # type: Parameters
    ):
        super(_BaseTask, self).__init__()
        self.task_definition = task_definition
        self.task_params = task_params
        self.task_name = task_name  # type: str

        # miscellaneous
        self.task_id = "{}__{}".format(task_name, task_signature_obj.signature)
        self.task_type = self._conf__task_type_name

        self.task_signature_obj = task_signature_obj
        self.task_outputs_signature_obj = None

        # define it at the time of creation
        # we can remove the strong reference, the moment we have global cache for instances
        # otherwise if we moved to another databand_context, task_id relationships will not be found
        self.dbnd_context = get_databand_context()

        # we count task meta creation
        # even if cached task is going to be used we will increase creation id
        # if t2 created after t1, t2.task_creation_id > t1.task_creation_id
        _BaseTask._current_task_creation_id += 1
        self.task_creation_id = _BaseTask._current_task_creation_id

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.task_id == other.task_id

    def __hash__(self):
        return hash(self.task_id)

    def __repr__(self):
        return "%s" % self.task_id

    def __str__(self):
        return self.task_id

    @property
    def friendly_task_name(self):
        if self.task_name != self.task_family:
            return "%s[%s]" % (self.task_name, self.task_family)
        return self.task_name

    @property
    def settings(self):
        # type: () -> DatabandSettings

        return self.dbnd_context.settings

    @property
    def task_signature(self):
        # type: () -> str
        return self.task_signature_obj.signature

    @property
    def task_signature_source(self):
        # type: () -> str
        return self.task_signature_obj.signature_source

    def __iter__(self):
        raise friendly_error.task_build.iteration_over_task(self)

    def _task_banner(self, banner, verbosity):
        """
        customize task banner
        """
        return
