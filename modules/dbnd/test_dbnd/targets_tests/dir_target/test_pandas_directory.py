from __future__ import print_function

import logging

from pandas.util.testing import assert_frame_equal

from targets import target
from targets.target_config import file
from test_dbnd.targets_tests import TargetTestBase


logger = logging.getLogger(__name__)


class TestPandasFrameTargetCtrl(TargetTestBase):
    def test_folder_simple(self, pandas_data_frame):
        t = self.target("dir/")
        df = pandas_data_frame
        t.as_pandas.to_parquet(df)
        actual = t.as_pandas.read_parquet()
        assert_frame_equal(actual, df)

    def test_folder_partitions_with_index_parquet(self, pandas_data_frame_index):
        t = self.target("dir/", config=file.parquet)
        df = pandas_data_frame_index
        df_len = len(pandas_data_frame_index)
        t.as_pandas.to_parquet((d for d in [df, df]))

        # with explicit index
        actual = t.read_df(set_index="Names")
        assert_frame_equal(actual.head(df_len), pandas_data_frame_index)

        actual = t.read_df()
        assert len(actual) == 2 * df_len
        assert_frame_equal(actual.head(df_len), pandas_data_frame_index)

    def test_folder_partitions_with_index_csv(self, pandas_data_frame_index):
        t = self.target("dir/", config=file.csv)
        df = pandas_data_frame_index
        df_len = len(df)
        df2 = df.copy()
        df2 = df2.rename(index=lambda x: x + "s")
        logger.info("original:\n%s", df)
        t.as_pandas.to_csv((d for d in [df, df2]))
        actual = t.read_df(index_col=False, set_index="Names")
        logger.info("expected:\n%s", actual)
        assert_frame_equal(actual.head(df_len), pandas_data_frame_index)

        a2 = t.partition("part-0001").read_df(index_col=False, set_index="Names")
        logger.info("partition:\n%s", a2)
        assert_frame_equal(a2, df2)

        actual = t.read_df(set_index="Names")
        print(actual)
        assert len(actual) == 2 * df_len
        assert_frame_equal(actual.head(df_len), pandas_data_frame_index)

    def test_read_pandas_csv(self, s1_dir_with_csv, simple_df):
        t = target(s1_dir_with_csv[0])
        actual = t.read_df()
        assert_frame_equal(actual, simple_df)
