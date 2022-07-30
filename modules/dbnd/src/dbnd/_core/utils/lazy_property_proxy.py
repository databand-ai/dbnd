# © Copyright Databand.ai, an IBM Company 2022

# inspired by wrapt.CallableObjectProxy
class CallableLazyObjectProxy(object):
    """
    Will proxy all attributes access and __call__ invocation to wrapped object
    returned by provided function.

    Provided function is evaluated only on access, allowing lazy evaluation.
    Provided function is evaluated each time to allow changing underlying
    wrapped object.
    """

    __slots__ = ["__wrapped__", "__weakref__"]

    def __init__(self, wrapped):
        self.__wrapped__ = wrapped

    @property
    def _wrapped(self):
        return self.__wrapped__()

    @property
    def __name__(self):
        return self._wrapped.__name__

    @property
    def __class__(self):
        return self._wrapped.__class__

    @property
    def __annotations__(self):
        return self._wrapped.__annotations__

    def __str__(self):
        return str(self._wrapped)

    def __repr__(self):
        return "<{} at 0x{:x} for {} at 0x{:x}>".format(
            type(self).__name__,
            id(self),
            type(self._wrapped).__name__,
            id(self._wrapped),
        )

    def __getattr__(self, item):
        return getattr(self._wrapped, item)

    def __call__(self, *args, **kwargs):
        return self._wrapped(*args, **kwargs)
