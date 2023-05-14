import importlib


def is_module_enabled(module):
    try:
        importlib.import_module(module_import)
        return True
    except import_errors:
        pass
    return False
