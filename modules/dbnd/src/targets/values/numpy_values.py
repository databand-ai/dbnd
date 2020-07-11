from __future__ import absolute_import

import numpy

from dbnd._vendor import fast_hasher
from targets.values.builtins_values import DataValueType


class NumpyArrayValueType(DataValueType):
    type = numpy.ndarray
    config_name = "numpy_ndarray"
    support_merge = True

    def to_signature(self, x):
        shape = "[%s]" % (",".join(map(str, x.shape)))
        return "%s:%s" % (shape, fast_hasher.hash(x))

    def merge_values(self, *values, **kwargs):
        import numpy as np

        return np.concatenate(values)
