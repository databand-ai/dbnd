# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.utils.basics.load_python_module import load_python_attr_from_module
from targets.marshalling.marshaller import Marshaller


logger = logging.getLogger(__name__)


class MarshallerLoader(Marshaller):
    """
    Use it only for simple data types that doesn't have is_instance_by_class_name overload
    """

    type = None

    def __init__(self, marshaller_class_name, package="unknown"):
        # legacy property
        self.package = package
        self.marshaller_class_name = marshaller_class_name
        self._marshaller = None
        self.load_error = None

    def load_marshaller(self):
        """
        Lazy loading value type
        """
        if self._marshaller is not None:
            return self._marshaller
        if self.load_error:
            # already failed to load previous time
            return None
        try:

            # Try to load module and check again for existance
            marshaller_cls = load_python_attr_from_module(self.marshaller_class_name)
            logger.debug(
                "loaded Marshaller %s for %s from %s",
                self.marshaller_class_name,
                self.marshaller_class_name,
                self.package,
            )
            # we do extra load_marshaller to support Structured value types
            self._marshaller = marshaller_cls().load_marshaller()
        except Exception as ex:
            self.load_error = ex
            log_exception(
                "Failed to load value type %s" % self.marshaller_class_name,
                ex=ex,
                non_critical=True,
            )
        return self._marshaller

    def __repr__(self):
        return f"MarshallerLoader<{self.marshaller_class_name}"


def dbnd_package_marshaller(marshaller_class):
    return MarshallerLoader(marshaller_class, package="dbnd'")
