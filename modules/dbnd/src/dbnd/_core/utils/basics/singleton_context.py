import logging
import sys

from contextlib import contextmanager
from typing import Any

from dbnd._core.errors import DatabandError, DatabandSystemError


logger = logging.getLogger(__name__)


class SingletonContext(object):
    """
    We need this object as we want to wrap __enter__ and __exit__
    function of every object with singleton management

    So inside 'with' statement we will get current object at get_instance()
    """

    _instance = None

    @classmethod
    def instance(cls):
        return cls.get_instance()

    @classmethod
    def get_instance(cls):
        """ Singleton getter """
        if not cls._instance:
            raise DatabandSystemError(
                "%s is not set, call .global_instance or .try_instance first"
                % cls.__name__
            )

        return cls._instance

    @classmethod
    def try_get_instance(cls):
        return cls._instance

    @classmethod
    def has_instance(cls):
        """ Singleton getter """
        return bool(cls._instance)

    @classmethod
    def try_instance(cls, *args, **kwargs):
        """ Singleton get or create"""
        if cls._instance is None:
            cls._instance = cls.new_context(*args, **kwargs).__enter__()
        return cls._instance

    @classmethod
    def renew_instance(cls, *args, **kwargs):
        cls._instance = None
        return cls.try_instance(*args, **kwargs)

    @classmethod
    @contextmanager
    def new_or_existing_context(cls, *args, **kwargs):
        if cls.has_instance():
            yield cls._instance
        else:
            with cls.new_context(*args, **kwargs) as new_context:
                yield new_context

    @classmethod
    @contextmanager
    def new_context(cls, *args, **kwargs):  # type: ('T', *Any, **Any)-> 'T'
        """
        Meant to be used as a context manager.
        """
        allow_override = kwargs.pop("allow_override", False)
        context = kwargs.pop("_context", None)

        orig_value = cls._instance
        if orig_value is not None:
            if not allow_override:
                raise DatabandSystemError(
                    "You are trying to create new %s out of existing context '%s', "
                    "are you sure you are allowed to do that? "
                    % (cls.__name__, cls._instance)
                )
        if context is None:
            try:
                context = cls(*args, **kwargs)
            except DatabandError:
                logger.error("Failed to create new context for %s", cls)
                raise
            except Exception:
                logger.exception("Failed to create new context for %s", cls)
                raise

        cls._instance = context
        _track_context(context, "enter")
        try:
            cls._instance._on_enter()
            yield cls._instance
        finally:
            _track_context(context, "exit")
            if cls._instance is not context:
                msg = (
                    "Something wrong with %s context manager, "
                    "somebody has change context while we were running: "
                    "actual=%s(%s), expected=%s(%s)"
                    % (
                        cls.__name__,
                        cls._instance,
                        id(cls._instance),
                        context,
                        id(context),
                    )
                )
                logger.warning(msg)
                raise DatabandSystemError(msg)

            cls._instance._on_exit()
            cls._instance = orig_value

    @classmethod
    @contextmanager
    def context(cls, _context, **kwargs):  # type: ('T', 'T', **Any)-> 'T'
        """
        Set current context to _context
        """
        with cls.new_context(_context=_context, allow_override=True, **kwargs) as c:
            yield c

    def _on_enter(self):
        pass

    def _on_exit(self):
        pass


def _track_context(context, op):
    # # uncomment it if we need to debug context before logging initialization
    # print(
    #     "\n\n-->CONTEXT: %s %s@%s" % (op, context, id(context)), file=sys.__stderr__
    # )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("%s@%s - %s", context, id(context), op)
