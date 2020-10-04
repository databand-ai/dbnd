from dbnd._core.utils.basics.nothing import NOTHING, is_defined


def passthrough_kwargs(current_locals):
    current_locals.pop("self", None)
    kwargs = current_locals.pop("kwargs", {})
    kwargs.update(current_locals)
    return kwargs


def _pop_kwarg(kwargs, key, default=NOTHING, error_msg=None):
    if key in kwargs:
        return kwargs.pop(key)
    if is_defined(default):
        return default
    raise TypeError(error_msg or "%s must be specified" % key)
