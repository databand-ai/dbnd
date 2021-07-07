import logging

from dbnd._core.parameter import PARAMETER_FACTORY


logger = logging.getLogger(__name__)


class ParametersMapper(object):
    def __init__(self):
        self.custom_parameters = {}

    def register_custom_parameter(self, value_type, parameter):
        self.custom_parameters[value_type] = parameter

    def get_parameter(self, value_type):
        custom_parameter = self.custom_parameters.get(value_type)
        if custom_parameter:
            return custom_parameter
        return PARAMETER_FACTORY.modify(value_type=value_type)
