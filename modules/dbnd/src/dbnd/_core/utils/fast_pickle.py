# © Copyright Databand.ai, an IBM Company 2022

from six.moves import cPickle as pickle

import dbnd._vendor.cloudpickle as cloudpickle


# noinspection PyBroadException
def dumps(o):
    try:
        return pickle.dumps(o, pickle.HIGHEST_PROTOCOL)
    except Exception:
        return cloudpickle.dumps(o)


# noinspection PyBroadException
def loads(o):
    """Unpickle using cPickle"""
    return pickle.loads(o)
