import os


def environ_enabled(variable_name, default=False):
    # type: (str, bool) -> bool
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
