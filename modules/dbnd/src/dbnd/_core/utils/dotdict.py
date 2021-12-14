import inspect


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def _as_dotted_dict(**dict_obj):
    return dotdict(**dict_obj)


def build_dict_from_instance_properties(class_instance) -> dict:
    """Retrunes dict of all class properties including @property methods"""
    properties = {}
    for prop_name in dir(class_instance):
        prop_value = getattr(class_instance, prop_name)
        if not prop_name.startswith("__") and not inspect.ismethod(prop_value):
            properties[prop_name] = prop_value
    return properties
