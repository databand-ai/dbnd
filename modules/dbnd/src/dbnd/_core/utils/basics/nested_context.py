from dbnd._core.utils.seven import contextlib


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
