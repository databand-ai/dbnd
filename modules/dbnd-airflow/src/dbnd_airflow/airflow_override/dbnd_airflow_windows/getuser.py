import os


def _default_getuser():
    return "unknown"


def find_runnable_getuser_function():
    try:

        if hasattr(os, "getlogin"):
            os.getlogin()
            return os.getlogin  # check before we return it

        import getpass

        if not hasattr(getpass, "getuser"):
            return _default_getuser()

        getpass.getuser()  # check before we return it
        return getpass.getuser
    except Exception:
        return _default_getuser
