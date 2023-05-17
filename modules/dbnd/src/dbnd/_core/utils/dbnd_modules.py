# © Copyright Databand.ai, an IBM Company 2022

import importlib


def is_module_enabled(module):
    try:
        importlib.import_module(module)
        return True
    except ImportError:
        pass
    return False
