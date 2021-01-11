import os

from contextlib import contextmanager


def environ_enabled(variable_name, default=False):
    # type: (str, Optional[bool]) -> Optional[bool]
    env_value = os.environ.get(variable_name, None)
    if env_value is None:
        return default

    from dbnd._core.utils.basics.helpers import parse_bool

    return parse_bool(env_value)


def environ_int(variable_name, default=None):
    # type: (str, Optional[int]) -> Optional[int]
    env_value = os.environ.get(variable_name, None)
    if env_value is None:
        return default
    try:
        return int(env_value)
    except:
        return default


def set_on(env_key):
    os.environ[env_key] = "True"


def set_env_dir(key, value):
    key = key.upper()
    if key not in os.environ:
        os.environ[key] = os.path.abspath(value)
        return True
    return False


@contextmanager
def env(**environment):
    """
    edit the environment variables only in the context scope
    """
    current = dict(os.environ)
    difference = {key for key in environment if key not in current}

    os.environ.update(environment)
    try:
        yield
    finally:
        os.environ.update(current)
        for key in difference:
            os.environ.pop(key, None)
