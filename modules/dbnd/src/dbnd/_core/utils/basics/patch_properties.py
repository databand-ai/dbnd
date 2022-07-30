# © Copyright Databand.ai, an IBM Company 2022

from contextlib import contextmanager

import six


@contextmanager
def patched_properties(obj, properties):
    """
    This method is a context-manager which can be called as below:

    .. code-block: python
                with no_unpicklable_properties(self,("some_property", patch_value)):
                    do_something(self)

    """
    patched_properties = {}
    new_properties = []
    for name, value in properties:
        if hasattr(obj, name):
            patched_properties[name] = getattr(obj, name)
        else:
            new_properties.append(name)

        setattr(obj, name, value)

    yield obj

    for name, value in six.iteritems(patched_properties):
        setattr(obj, name, value)

    for name in new_properties:
        delattr(obj, name)
