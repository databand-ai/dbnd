# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.errors import friendly_error
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.utils.basics.load_python_module import load_python_attr_from_module
from targets.values import ValueType


logger = logging.getLogger(__name__)


class ValueTypeLoader(ValueType):
    """
    Use it only for simple data types that doesn't have is_instance_by_class_name overload
    """

    support_discover_from_obj = True
    load_on_build = False
    type = None

    def __init__(
        self, class_name, value_type_class_name, package, type_str_extras=None
    ):
        # legacy property
        super().__init__()
        self.type_str_extras = type_str_extras or []

        self.class_name = class_name
        self.package = package
        self.value_type_class_name = value_type_class_name
        self._value_type = None
        self.load_error = None

    @property
    def type_str(self):
        return self.class_name

    def load_value_type(self):
        """
        Lazy loading value type
        """
        if self._value_type is not None:
            return self._value_type
        if self.load_error:
            # already failed to load previous time
            return None
        try:

            # Try to load module and check again for existance
            value_type_cls = load_python_attr_from_module(self.value_type_class_name)
            logger.debug(
                "loaded ValueType %s for %s from %s",
                self.value_type_class_name,
                self.class_name,
                self.package,
            )
            # we do extra load_value_type to support Structured value types
            value_type = value_type_cls()
            self._value_type = value_type.load_value_type()
            self._value_type.marshallers.update(self.marshallers)
            return self._value_type
        except Exception as ex:
            self.load_error = ex
            log_exception(
                "Failed to load value type %s" % self.class_name,
                ex=ex,
                non_critical=True,
            )
            return None

    def is_handler_of_type(self, class_):
        if class_ is None:
            return None
        if hasattr(class_, "__qualname__"):
            module = class_.__module__
            if module != "builtins":
                class_name = module + "." + class_.__qualname__
            else:  # don't add things for builtin
                class_name = class_.__qualname__
        else:  # generic case
            class_name = str(class_)
        return class_name == self.class_name or class_name in self.type_str_extras

    def parse_value(self, value, **kwargs):
        raise friendly_error.task_parameters.value_type_is_not_loaded(self)

    def is_type_of(self, value):
        return self.is_handler_of_type(type(value))

    def __repr__(self):
        return "ValueTypeLoader<%s>" % (self.class_name)
