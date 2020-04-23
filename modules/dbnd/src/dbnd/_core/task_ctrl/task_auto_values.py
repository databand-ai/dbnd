import logging
import typing

from contextlib import contextmanager
from typing import Any

import attr

from dbnd._core.constants import DbndTargetOperationStatus, DbndTargetOperationType
from dbnd._core.current import try_get_current_task_run
from dbnd._core.errors import friendly_error
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from targets import InMemoryTarget
from targets.values import get_value_type_of_obj


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.task.base_task import _BaseTask


@attr.s
class TaskAutoParamsReadWrite(object):
    task = attr.ib()  # type: _BaseTask
    auto_read = attr.ib()
    save_on_change = attr.ib()
    normalize_on_change = attr.ib()

    @contextmanager
    def auto_load_save_params(self):
        task = self.task
        original_values = task._params.get_param_values()

        # we don't support "nested" calls for now,
        # let's not overcomplicate code for non existing scenario

        if task._task_auto_read is not None:
            logger.warning(
                "You are running in {task} within already existing TaskAutoParamsReadWrite context".format(
                    task=task
                )
            )

        if self.auto_read:
            task._task_auto_read_original = {p.name: v for p, v in original_values}
            task._task_auto_read = set()

        dirty = self.save_on_change
        try:
            yield original_values
            # now we disable "auto read"
            task._task_auto_read = None
            task._task_auto_read_original = None
            # from here we are going to read "the value" without autoresolving

            current_values = {
                p.name: value for p, value in task._params.get_param_values()
            }
            if not self.save_on_change and not self.normalize_on_change:
                return

            changed = []
            for p, original_value in original_values:
                current_value = current_values[p.name]
                if id(original_value) != id(current_value):
                    # nothing to do original_value is the same
                    changed.append((p, original_value, current_value))

            if self.save_on_change:
                try:
                    for p, original_value, current_value in changed:
                        # TODO: implement Atomic commit
                        if p.is_output():
                            self.auto_save_param(p, original_value, current_value)
                finally:
                    for p, original_value, current_value in changed:
                        setattr(task, p.name, original_value)
                    dirty = False

            elif self.normalize_on_change:
                for p, original_value, current_value in changed:
                    try:
                        # probably we are in the band
                        # we are going just to normalize the value
                        if p.is_output():
                            from dbnd._core.utils.task_utils import to_targets

                            normalized_value = to_targets(current_value)
                        else:
                            normalized_value = p.normalize(current_value)
                        if id(normalized_value) != id(current_value):
                            setattr(task, p.name, normalized_value)
                    except Exception as ex:
                        raise friendly_error.task_build.failed_to_assign_param_value_at_band(
                            ex, p, current_value, task
                        )
        finally:
            task._task_auto_read = None
            if dirty:
                for p, original_value in original_values:
                    setattr(task, p.name, original_value)

    def auto_save_param(self, parameter, original_value, current_value):
        # type: (ParameterDefinition, Any, Any) -> None
        # it's output! we are going to save it.
        # task run doesn't always exist
        task_run = try_get_current_task_run()
        access_status = DbndTargetOperationStatus.OK
        try:
            if isinstance(original_value, InMemoryTarget):
                parameter.value_type = get_value_type_of_obj(
                    current_value, parameter.value_type
                )

            parameter.dump_to_target(original_value, current_value)
            # it's a workaround, we don't want to change parameter for outputs (dynamically)
            # however, we need proper value type to "dump" preview an other meta.
            # we will update it only for In memory targets only for now

        except Exception as ex:
            access_status = DbndTargetOperationStatus.NOK
            raise friendly_error.task_execution.failed_to_save_value_to_target(
                ex, self.task, parameter, original_value, current_value
            )
        finally:
            if task_run:
                try:
                    task_run.tracker.log_target(
                        parameter=parameter,
                        target=original_value,
                        value=current_value,
                        operation_type=DbndTargetOperationType.write,
                        operation_status=access_status,
                    )
                except Exception as ex:
                    logger.warning("Failed to log target to tracking store. %s", ex)
