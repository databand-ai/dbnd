# © Copyright Databand.ai, an IBM Company 2022

from contextlib import contextmanager

from dbnd._core.utils.basics.patch_properties import patched_properties


@contextmanager
def ready_for_pickle(obj, unpicklable_properties):
    """
    Remove unpicklable properties before dump obj and resume them after.
    This method could be called in dump method, to ensure unpicklable properties won't break dump.
    This method is a context-manager which can be called as below:

    .. code-block: python

        class MyObj(object):

            def _dump(self):
                with no_unpicklable_properties(self,("some_property",)):
                    pickle.dumps(self)

    """
    patch_properties = []
    for p in unpicklable_properties:
        if isinstance(p, tuple):
            property_name, property_value = p[0], p[1]
        else:
            property_name, property_value = p, "_non_pickable_property_"
        if hasattr(obj, property_name):
            patch_properties.append((property_name, property_value))

    with patched_properties(obj, patch_properties) as obj:
        yield obj
