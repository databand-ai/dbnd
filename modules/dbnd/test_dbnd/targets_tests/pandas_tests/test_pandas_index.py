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

    def create_target(self, **kwargs):
        return target(self.path, **kwargs)

    def test_simple_csv(self, pandas_data_frame):
        t = self.create_target()

        expected = pandas_data_frame
        t.as_pandas.to_csv(expected, index=False)
        assert_frame_equal(t.as_pandas.read_csv(), expected)

        # validate real read
        actual = target(t.path).as_pandas.read_csv(index_col=None)

        print(expected)
        print(actual)

        assert_frame_equal(actual, expected)
