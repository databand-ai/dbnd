from dbnd._core.errors import friendly_error
from dbnd._core.utils import json_utils
from targets.base_target import Target
from targets.types import Path, PathStr
from targets.values.value_type import ValueType


class _TargetValueType(ValueType):  # extracted for generics support
    """
      ValueType whose value is a ```target```.

      In the task definition, use

      .. code-block:: python

          class MyTask(dbnd.Task):
              some_input = TargetValueType()

              def run(self):
                    self.some_input.open('r')

      """

    support_from_str = False
    config_name = "target"

    def normalize(self, value):
        from dbnd._core.utils.task_utils import to_targets

        return to_targets(value)

    def normalize_to_target(self, value):
        from dbnd._core.utils.task_utils import to_targets

        return to_targets(value)

    def to_str(self, x):
        # value should be the "deferred value" -> Target of any kind
        return json_utils.dumps_canonical(x)

    def save_to_target(self, target, value, **kwargs):
        # if data - automatic mode, we don't give value_type.
        # That makes parameter is agnostic to data type.
        target.dump(value, value_type=None, **kwargs)

    def target_to_value(self, target):
        return target

    def parse_value(self, value, load_value=None, target_config=None, sub_value=False):
        value = super(_TargetValueType, self).parse_value(
            value, load_value=load_value, target_config=target_config
        )
        try:
            return self.normalize_to_target(value)
        except Exception as ex:
            raise friendly_error.task_parameters.failed_to_convert_to_target_type(
                self, value, ex
            )

    def load_from_target(self, target, **kwargs):
        return self.target_to_value(target)


class TargetValueType(_TargetValueType):  # _TargetValueType[DataTarget]
    type = Target
    load_on_build = False
    type_str_extras = ("DataTarget", "FileTarget", "DirTarget")

    def is_handler_of_type(self, type_):
        return super(TargetValueType, self).is_handler_of_type(type_) or issubclass(
            type_, Target
        )

    def is_type_of(self, value):
        return isinstance(value, Target)

    def target_to_value(self, target):
        return target


class TargetPathValueType(_TargetValueType):  # _TargetValueType[PathStr]
    type = PathStr
    load_on_build = False

    def target_to_value(self, target):
        return target.path


class TargetPathLibValueType(_TargetValueType):  # _TargetValueType[PathStr]
    type = Path
    load_on_build = False

    def target_to_value(self, target):
        return Path(target.path)
