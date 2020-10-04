from targets.target_config import TargetConfig, parse_target_config
from targets.values import ValueType


class TargetConfigValueType(ValueType):  # extracted for generics support
    """
      ValueType whose value is a ```target```.

      In the task definition, use

      .. code-block:: python

          class MyTask(dbnd.Task):
              some_input = TargetValueType()

              def run(self):
                    self.some_input.open('r')

      """

    type = TargetConfig

    support_from_str = True

    def parse_from_str(self, x):
        return parse_target_config(x)
