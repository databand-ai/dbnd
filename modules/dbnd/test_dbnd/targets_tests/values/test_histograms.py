from __future__ import print_function

import logging

import pandas as pd

from dbnd_test_scenarios.test_common.histogram_tests import BaseHistogramTests


logger = logging.getLogger(__name__)


def get_value_meta_from_value(name, value, meta_conf):
    from dbnd._core.settings import TrackingConfig
    from dbnd._core.settings.tracking_config import ValueTrackingLevel, get_value_meta

    c = TrackingConfig.from_databand_context()
    c.value_reporting_strategy = ValueTrackingLevel.ALL
    return get_value_meta(value, meta_conf, tracking_config=c)


class TestPandasHistograms(BaseHistogramTests):
    def data_to_value(self, data):
        dict_data = {"test_column_" + str(i): column for i, column in enumerate(data)}
        return pd.DataFrame(dict_data)

    def test_null_column(self, meta_conf, numbers_value):
        nulls = [None] * 20
        df = self.data_to_value([nulls])
        value_meta = get_value_meta_from_value("nulls", df, meta_conf)

        histogram = value_meta.histograms["test_column_0"]
        assert histogram[0] == [20]
        assert histogram[1] == [None]

        col_stats = value_meta.get_column_stats_by_col_name("test_column_0")
        assert col_stats.records_count == 20
        assert col_stats.non_null_count == 0
        assert col_stats.null_count == 20
        assert col_stats.distinct_count == 1
        assert col_stats.column_type == "object"
