import os

from dbnd._core.configuration.environ_config import ENV_DBND_USER


def _default_getuser():
    return "unknown"


def _find_getuser_f():
    try:
        if ENV_DBND_USER in os.environ:
            return lambda: os.environ[ENV_DBND_USER]

        if os.name == "nt":
            if hasattr(os, "getlogin"):
                os.getlogin()
                return os.getlogin

            import getpass

            if not hasattr(getpass, "getuser"):
                return _default_getuser()
            # check before we return it
            getpass.getuser()

        import getpass

        return getpass.getuser
    except Exception:
        return _default_getuser


dbnd_getuser = _find_getuser_f()
