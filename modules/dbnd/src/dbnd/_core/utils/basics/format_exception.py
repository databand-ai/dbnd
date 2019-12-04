import sys
import traceback


def format_exception_as_str(exc_info=None):
    if not exc_info:
        exc_info = sys.exc_info()

    exc_type, exc_value, exc_traceback = exc_info
    lines = traceback.format_exception(
        type(exc_value), exc_value, exc_traceback, limit=None
    )
    return "".join(lines)
