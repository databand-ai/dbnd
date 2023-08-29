# © Copyright Databand.ai, an IBM Company 2022
import logging

from dbnd._core.log import dbnd_log_exception
from dbnd._core.utils.seven import contextlib


logger = logging.getLogger(__name__)

# noinspection PyBroadException
@contextlib.contextmanager
def nested(*managers):
    """Combine multiple context managers into a single nested context manager.

    The one advantage of this function over the multiple manager form of the
    with statement is that argument unpacking allows it to be
    used with a variable number of context managers as follows:

       with nested(*managers):
           do_something()

    """
    if not managers:
        yield
        return

    with contextlib.ExitStack() as stack:
        for mgr in managers:
            if mgr is None:
                continue
            stack.enter_context(mgr)
        yield


@contextlib.contextmanager
def safe_nested(*managers):
    """
     Combine multiple context managers into a single nested context manager.
     All exceptions will be muted.

    with safe_nested(ctx_manager_a, ctx_manager_b):
        do_something()

    """
    if not managers:
        yield
        return

    with contextlib.ExitStack() as stack:
        for mgr in managers:
            if mgr is None:
                continue
            try:
                stack.enter_context(mgr)
            except Exception as e:
                dbnd_log_exception(f"Exception caught while entering {mgr}: {e}")
        try:
            yield

        # Do not mute exception that occurs inside nested(user) code,
        # do not capture exception here , and raise
        finally:
            try:
                stack.close()
            except Exception as e:
                dbnd_log_exception(f"Exception caught while exiting: {e}")
