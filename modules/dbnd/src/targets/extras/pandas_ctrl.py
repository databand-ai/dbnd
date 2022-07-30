# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Any

from pandas import DataFrame

from targets.extras import DataTargetCtrl
from targets.marshalling import get_marshaller_ctrl
from targets.target_config import FileFormat, file
from targets.utils.performance import target_timeit


logger = logging.getLogger(__name__)
file_table = file.with_format(FileFormat.table)


class PandasMarshallingCtrl(DataTargetCtrl):
    def __init__(self, target):
        super(PandasMarshallingCtrl, self).__init__(target)

    def read(self, config=None, **kwargs):
        # type: (file, **Any) -> 'DataFrame'
        pd_m = get_marshaller_ctrl(self.target, DataFrame, config=config)
        return pd_m.load(**kwargs)

    def read_partitioned(self, config=None, **kwargs):
        pd_m = get_marshaller_ctrl(self.target, DataFrame, config=config)
        for t in pd_m.load_partitioned(**kwargs):
            yield t

    @target_timeit
    def to(self, df, config=None, **kwargs):
        pd_m = get_marshaller_ctrl(self.target, value_type=DataFrame, config=config)
        return pd_m.dump(df, **kwargs)

    def read_csv(self, **kwargs):
        return self.read(config=file.csv, **kwargs)

    def read_table(self, **kwargs):
        return self.read(config=file_table, **kwargs)

    def read_hdf(self, **kwargs):
        return self.read(config=file.hdf5, **kwargs)

    def read_json(self, **kwargs):
        return self.read(config=file.json, **kwargs)

    def read_feather(self, **kwargs):
        return self.read(config=file.feather, **kwargs)

    def read_parquet(self, **kwargs):
        return self.read(config=file.parquet, **kwargs)

    def read_pickle(self, **kwargs):
        return self.read(config=file.pickle, **kwargs)

    def to_csv(self, df, **kwargs):
        return self.to(df, config=file.csv, **kwargs)

    def to_json(self, df, **kwargs):
        return self.to(df, config=file.json, **kwargs)

    def to_hdf(self, df, **kwargs):
        return self.to(df, config=file.hdf5, **kwargs)

    def to_feather(self, df, **kwargs):
        return self.to(df, config=file.feather, **kwargs)

    def to_table(self, df, **kwargs):
        return self.to(df, config=file_table, **kwargs)

    def to_parquet(self, df, **kwargs):
        return self.to(df, config=file.parquet, **kwargs)

    def to_pickle(self, df, **kwargs):
        return self.to(df, config=file.pickle, **kwargs)
