# © Copyright Databand.ai, an IBM Company 2022

import inspect


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def _as_dotted_dict(**dict_obj):
    return dotdict(**dict_obj)


def build_dict_from_instance_properties(class_instance) -> dict:
    """Returns dict of all class properties including @property methods"""
    properties = {}
    for prop_name in dir(class_instance):
        prop_value = getattr(class_instance, prop_name)
        if not prop_name.startswith("__") and not inspect.ismethod(prop_value):
            properties[prop_name] = prop_value
    return properties


class rdotdict(dict):
    """
    this is "recursive" dotdict - provides dot-notation access to dict, and recursively
    adds itself for internal values (dicts and lists). So this code is valid:
    ```
    d = {"a": [{"b": 42}]}
    rdotdict(d).a[0].b == 42
    ```
    """

    def __getattr__(self, item):
        return self[item]

    def __getitem__(self, item):
        return rdotdict.try_create(dict.__getitem__(self, item))

    @staticmethod
    def try_create(val):
        """Applies rdotdict for rdotdict-able object (i.e dict or list)"""
        if isinstance(val, dict):
            return rdotdict(**val)
        if isinstance(val, list):
            return [rdotdict.try_create(x) for x in val]
        return val
