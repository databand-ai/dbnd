# © Copyright Databand.ai, an IBM Company 2022

from typing import List

from targets.extras import DataTargetCtrl
from targets.marshalling import get_marshaller_ctrl
from targets.target_config import file


class ObjectMarshallingCtrl(DataTargetCtrl):
    def _load(self, value_type, config, **kwargs):
        marshaler_ctrl = get_marshaller_ctrl(
            target=self.target, value_type=value_type, config=config
        )
        return marshaler_ctrl.load(**kwargs)

    def _dump(self, value_type, config, value, **kwargs):
        marshaler_ctrl = get_marshaller_ctrl(
            target=self.target, value_type=value_type, config=config
        )
        return marshaler_ctrl.dump(value, **kwargs)

    # aliases fro the most used functions - do things regardless file type
    def readlines(self, **kwargs):
        return self._load(List[str], file.txt, **kwargs)

    def read(self, **kwargs):
        return self._load(object, file.txt, **kwargs)

    def read_obj(self, **kwargs):
        return self._load(object, file.pickle, **kwargs)

    def read_json(self, **kwargs):
        return self._load(object, file.json, **kwargs)

    def read_pickle(self, **kwargs):
        return self._load(object, file.pickle, **kwargs)

    def write(self, fn, **kwargs):
        self._dump(object, file.txt, fn, **kwargs)

    def writelines(self, fn, **kwargs):
        self._dump(List[str], file.txt, fn, **kwargs)

    def write_pickle(self, obj, **kwargs):
        self._dump(object, file.pickle, obj, **kwargs)

    def write_numpy_array(self, arr, **kwargs):
        import numpy

        self._dump(numpy.ndarray, file.numpy, arr, **kwargs)

    def write_json(self, obj, **kwargs):
        self._dump(object, file.json, obj, **kwargs)

    def touch(self):
        with self.target.open("w"):
            pass
