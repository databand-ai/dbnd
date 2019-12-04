import sys

from contextlib import contextmanager


# noinspection PyBroadException
@contextmanager
def nested(*managers):
    """Combine multiple context managers into a single nested context manager.

   Standard python library code ( original is going to be deprecated)

   The one advantage of this function over the multiple manager form of the
   with statement is that argument unpacking allows it to be
   used with a variable number of context managers as follows:

      with nested(*managers):
          do_something()

    """
    exits = []
    vars = []
    exc = (None, None, None)
    try:
        for mgr in managers:
            exit = mgr.__exit__
            enter = mgr.__enter__
            vars.append(enter())
            exits.append(exit)
        yield vars
    except:  # noqa: E722
        exc = sys.exc_info()
    finally:
        while exits:
            exit = exits.pop()
            try:
                if exit(*exc):
                    exc = (None, None, None)
            except:  # noqa: E722
                exc = sys.exc_info()
        if exc != (None, None, None):
            # Don't rely on sys.exc_info() still containing
            # the right information. Another exception may
            # have been raised and caught by an exit method
            raise exc[1]
