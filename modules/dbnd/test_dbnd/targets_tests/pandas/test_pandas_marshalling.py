from __future__ import print_function

import logging

import pandas as pd
import pytest

from pandas.util.testing import assert_frame_equal, assert_series_equal

from targets.marshalling import (
    DataFrameToCsv,
    DataFrameToFeather,
    DataFrameToHdf5,
    DataFrameToPickle,
    DataFrameToTsv,
)
from test_dbnd.targets_tests import TargetTestBase


logger = logging.getLogger(__name__)


class TestPandasMarshalling(TargetTestBase):
    def test_dataframe_marshalling_csv(self, pandas_data_frame):
        t = self.target("df.csv")
        df = pandas_data_frame
        df_to_csv = DataFrameToCsv()
        df_to_csv.value_to_target(df, t)
        assert t.exists()

        actual = df_to_csv.target_to_value(t)
        assert_frame_equal(actual, df)

    def test_dataframe_marshalling_tsv(self, pandas_data_frame):
        t = self.target("df.tsv")
        df = pandas_data_frame
        df_to_tsv = DataFrameToTsv()
        df_to_tsv.value_to_target(df, t)
        assert t.exists()

        actual = df_to_tsv.target_to_value(t)
        assert_frame_equal(actual, df)

    def test_series_marshalling_csv(self):
        t = self.target("p_series.csv")

        p_series = pd.Series([1, 3, 5, 6, 8])

        df_to_csv = DataFrameToCsv(series=True)
        df_to_csv.value_to_target(p_series, t)
        assert t.exists()

        actual = df_to_csv.target_to_value(t)
        assert_series_equal(actual, p_series, check_names=False)

    @pytest.mark.parametrize("marshaller_cls", [DataFrameToHdf5, DataFrameToPickle])
    def test_series_marshalling(self, marshaller_cls):
        t = self.target("p_series")

        p_series = pd.Series([1, 3, 5, 6, 8])

        df_to_csv = marshaller_cls()
        df_to_csv.value_to_target(p_series, t)
        assert t.exists()

        actual = df_to_csv.target_to_value(t)
        assert_series_equal(actual, p_series)
