from __future__ import print_function

import logging

import pandas as pd

from dbnd_test_scenarios.histograms.histogram_tests import BaseHistogramTests


logger = logging.getLogger(__name__)


def get_value_meta_from_value(name, value, meta_conf):
    from dbnd._core.settings import TrackingConfig
    from dbnd._core.settings.tracking_config import ValueTrackingLevel
    from dbnd._core.settings.tracking_config import get_value_meta

    c = TrackingConfig.current()
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

        stats = value_meta.descriptive_stats["test_column_0"]
        assert stats["count"] == 20
        assert stats["non-null"] == 0
        assert stats["null-count"] == 20
        assert stats["distinct"] == 1
        assert stats["type"] == "object"
