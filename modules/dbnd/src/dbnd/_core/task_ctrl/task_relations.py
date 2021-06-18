import logging
import sys
import warnings

from dbnd._core.errors import DatabandSystemError, friendly_error
from dbnd._core.errors.friendly_error import _band_call_str
from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.task_build.task_results import FuncResultParameter
from dbnd._core.task_build.task_signature import (
    build_signature,
    build_signature_from_values,
)
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.basics.nothing import is_not_defined
from dbnd._core.utils.task_utils import to_targets
from dbnd._core.utils.traversing import traverse
from targets import Target
from targets.base_target import TargetSource


logger = logging.getLogger(__name__)


def _section(parameter):
    return "system" if parameter.system else "user"


class TaskRelations(TaskSubCtrl):
    def __init__(self, task):
        super(TaskRelations, self).__init__(task)
        from dbnd._core.task.task_mixin import _TaskCtrlMixin

        # map of required targets
        self.task_inputs = None
        self.task_outputs = None

        self.task_band_result = None  # real output of task_band

        assert isinstance(task, _TaskCtrlMixin)

    def initialize_relations(self):
        # STEP 0 - run band function
        self.initialize_band()

        # STEP 1 - calculate all inputs and _required
        try:
            self.task_inputs = self.initialize_required()
        except Exception:
            logger.warning(
                "Failed to calculate relationships for %s" % self.task_id, exc_info=True
            )
            self.task_inputs = {}
            if not self.task.task_is_dynamic:
                raise

        # STEP 2 ( now we have all inputs, we can calculate real signature)
        # support for two phase build
        # will be called from MetaClass

        params = self.params.get_params_serialized(ParameterFilters.SIGNIFICANT_INPUTS)

        if "user" in self.task_inputs:
            # TODO : why do we need to convert all "user side" inputs?
            # what if the input is insignificant?
            system_input = self.task_inputs.get("system")
            if system_input and "band" in system_input:
                band_input = system_input["band"]
                task_inputs_user_only = {
                    "user": self.task_inputs.get("user"),
                    "system": {"band": band_input},
                }
            else:
                task_inputs_user_only = {"user": self.task_inputs.get("user")}
            task_inputs_as_str = traverse(
                task_inputs_user_only,
                convert_f=str,
                filter_none=True,
                filter_empty=True,
            )

            if task_inputs_as_str is None:
                task_inputs_as_str = ""

            params.append(("_task_inputs", task_inputs_as_str))

        # IMPORTANT PART: we initialize task_id here again
        # after all values are calculated (all task_inputs are assigned)
        # we do it again, now we have all inputs calculated
        task = self.task
        task.task_signature_obj = build_signature(
            name=task.task_name,
            params=params,
            extra=task.task_definition.task_signature_extra,
        )
        task.task_id = "{}__{}".format(
            task.task_name, task.task_signature_obj.signature
        )

        # for airflow operator task handling:
        airflow_task_id_p = self.params.get_param("airflow_task_id")
        if airflow_task_id_p:
            self.task.task_id = self.task.airflow_task_id

        # STEP 3  - now let update outputs
        self.initialize_outputs()

        outputs_sig = self._get_outputs_to_sign()
        if outputs_sig:
            sig = build_signature_from_values("task_outputs", outputs_sig)
            task.task_outputs_signature_obj = sig
        else:
            task.task_outputs_signature_obj = task.task_signature_obj

    def _get_outputs_to_sign(self):
        outputs_to_sign = self.task_outputs_user
        from dbnd import PipelineTask

        if not isinstance(self.task, PipelineTask):
            outputs_to_sign = {
                k: v for k, v in outputs_to_sign.items() if k != "task_band"
            }
        return outputs_to_sign

    def initialize_band(self):
        try:
            band_context = []
            if is_airflow_enabled():
                from dbnd_airflow.dbnd_task_executor.airflow_operators_catcher import (
                    get_databand_op_catcher_dag,
                )

                band_context.append(get_databand_op_catcher_dag())

            original_param_values = []
            for param_value in self.task.task_params.get_param_values(
                ParameterFilters.OUTPUTS
            ):
                if param_value.name == "task_band" or isinstance(
                    param_value.parameter, FuncResultParameter
                ):
                    continue
                original_param_values.append((param_value, param_value.value))

            with nested(*band_context):
                band = self.task.band()
                # this one would be normalized
                self.task._task_band_result = band
            self.task_band_result = band  # real value

            from dbnd import PipelineTask

            if isinstance(self.task, PipelineTask):
                # after .band has finished, all user outputs of the .band should be defined
                for param_value, _ in original_param_values:
                    # we want to validate only user facing parameters
                    # they should have assigned values by this moment,
                    # pipeline task can not have None outputs, after band call
                    if param_value.parameter.system:
                        continue
                    if is_not_defined(param_value.value):
                        raise friendly_error.task_build.pipeline_task_has_unassigned_outputs(
                            task=self.task, param=param_value.parameter
                        )

            # now let's normalize if user has changed outputs
            for param_value, original_value in original_param_values:
                if param_value.value is original_value:
                    continue

                try:
                    from dbnd._core.utils.task_utils import to_targets

                    normalized_value = to_targets(param_value.value)
                    param_value.update_param_value(normalized_value)
                except Exception as ex:
                    raise friendly_error.task_build.failed_to_assign_param_value_at_band(
                        ex, param_value.parameter, param_value.value, self.task
                    )

        except Exception as ex:
            logger.warning(
                self.visualiser.banner(
                    msg="Failed to run %s" % _band_call_str(self.task),
                    color="red",
                    exc_info=sys.exc_info(),
                )
            )

            if self.task.task_decorator:
                # just re-raise, we already have an error from the "run" function
                raise

            raise friendly_error.task_build.failed_to_call_band(ex, self.task)

    def _get_all_child_pipelines(self):
        from dbnd import PipelineTask

        child_pipelines = []
        for child in self.task.descendants.children:
            child_task = self.get_task_by_task_id(child)
            if isinstance(child_task, PipelineTask):
                child_pipelines.append(child_task)
        return child_pipelines

    def initialize_required(self):
        # regular requirements -- just all inputs
        inputs = {"user": {}, "system": {}}

        # we take all parameters that are inputs (not outputs)
        # however Primitive parameters are inputs only if they are Target (deferred)
        #           if isinstance(p, _TargetParameter) or isinstance(value, Target)

        for p, value in self.params.get_params_with_value(ParameterFilters.INPUTS):
            if value is None:
                continue
            value = traverse(
                value, convert_f=_find_target, filter_none=True, filter_empty=True
            )

            if not value:
                continue

            inputs[_section(p)][p.name] = value

        def _extend_system_section(key, extra):
            if not extra:
                return
            inputs["system"][key] = extra

        from dbnd import PipelineTask

        if isinstance(self.task, PipelineTask):
            task_output_values = {}
            for p, value in self.params.get_params_with_value(
                ParameterFilters.USER_OUTPUTS
            ):
                if p.name == "task_band" or isinstance(p, FuncResultParameter):
                    continue

                # band outputs are going to be required as inputs!
                # @pipeline can run only when all of it's "outputs" are ready
                task_output_values[p.name] = value

            _extend_system_section("band", task_output_values)

        # find all child pipelines and make them upstreams to the task
        _extend_system_section(
            "pipelines", {p.task_id: p for p in self._get_all_child_pipelines()}
        )
        # now may be user still use function _requires - so let add that to dependencies
        _extend_system_section("required", self.task._requires())

        return to_targets(inputs)

    def initialize_outputs(self):
        """
        The default output that this Task produces. Use outputs! Override only if you are writing "base" class
        """
        task = self.task

        outputs = {"user": {}, "system": {}}

        for p, value in self.params.get_params_with_value(ParameterFilters.OUTPUTS):
            if is_not_defined(value):
                value = p.build_output(task=task)
                setattr(self.task, p.name, value)

            if isinstance(p, FuncResultParameter):
                continue

            value = traverse_and_set_target(value, p._target_source(self.task))
            outputs[_section(p)][p.name] = value

        custom_outputs = self.task._output()
        if custom_outputs:
            if outputs["user"]:
                warnings.warn(
                    "Task %s has custom outputs in _output() function, all other outputs will be removed: %s"
                    % (task, outputs["user"]),
                    stacklevel=2,
                )
                outputs["user"] = custom_outputs

        # take ownership of all outputs and clean it, just in case
        # usually all outputs are assigned to task

        # just in case we have some "outputs" with Tasks
        outputs = to_targets(outputs)
        self.task_outputs = traverse_and_set_target(
            outputs, target_source=TargetSource(task_id=self.task_id)
        )
        # if not output.get('user', None):
        #
        #     raise DatabandBuildError("Task %s doesn't have any outputs defined." % task,
        #                              help_msg="1. Define output fields in Task class definition. \n"
        #                                       "2. If you have overrided Task._output() function "
        #                                       "it should have non empty return value")

    @property
    def task_inputs_user(self):
        return self.task_inputs.get("user", {})

    @property
    def task_inputs_system(self):
        return self.task_inputs.get("system", {})

    @property
    def task_outputs_user(self):
        return self.task_outputs.get("user", {})

    @property
    def task_outputs_system(self):
        return self.task_outputs.get("system", {})


def _find_target(target):
    if target is None:
        return target

    if isinstance(target, Target):
        return target
    return None


def traverse_and_set_target(target, target_source):
    return traverse(
        target, convert_f=lambda t: __set_target(target=t, target_source=target_source)
    )


def __set_target(target, target_source):
    if not target:
        return target

    if not isinstance(target, Target):
        raise DatabandSystemError(
            "Expected target object, got '%s' : %s" % (type(target), target)
        )
    if not target.source:
        target.source = target_source
    return target


def as_task(task_or_result):
    from dbnd import Task

    if isinstance(task_or_result, Target):
        return task_or_result.source_task
    if isinstance(task_or_result, Task):
        return task_or_result
    raise DatabandSystemError("Can not extract task from %s" % task_or_result)
