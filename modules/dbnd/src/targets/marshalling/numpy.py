from __future__ import absolute_import

import numpy

from targets.marshalling.marshaller import Marshaller
from targets.target_config import FileFormat


class NumpyArrayMarshaller(Marshaller):
    type = numpy.ndarray
    file_format = FileFormat.numpy

    def target_to_value(self, target, **kwargs):
        """
        :param obj: object to pickle
        :return:
        """
        import numpy as np

        with target.open("rb") as fp:
            return np.load(fp, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        """
        :param obj: object to pickle
        :return:
        """
        import numpy as np

        target.mkdir_parent()
        with target.open("wb") as fp:
            np.save(fp, value, **kwargs)


class NumpyArrayPickleMarshaler(NumpyArrayMarshaller):
    file_format = FileFormat.pickle

    def target_to_value(self, target, **kwargs):
        import numpy as np

        return np.load(target.path, allow_pickle=True, **kwargs)

    def value_to_target(self, value, target, **kwargs):
        import numpy as np

        np.save(target.path, value, allow_pickle=True, **kwargs)
