# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging

import pytest

from pandas.util.testing import assert_frame_equal

from targets import target


logger = logging.getLogger(__name__)


class TestPandasFrameTargetCtrl(object):
    @pytest.fixture(autouse=True)
    def set_paths_for_test(self, tmpdir):
        local_file = str(tmpdir.join("local_file"))
        self.path = local_file

    def test_cache(self, pandas_data_frame):
        t = target(self.path)
        df = pandas_data_frame
        t.as_pandas.to_parquet(df, cache=True)
        df_cache = t.as_pandas.read_parquet(no_copy_on_read=True)
        assert id(df) == id(df_cache)

        # validate real read
        actual = target(t.path).as_pandas.read_parquet()

        assert_frame_equal(actual, df)

    def test_no_cache_for_csv(self, pandas_data_frame):
        t = target(self.path)

        df = pandas_data_frame
        t.as_pandas.to_csv(df, index=False, cache=True)
        from_cache = t.as_pandas.read_csv(no_copy_on_read=True)
        assert id(from_cache) != id(df)
