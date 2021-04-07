from __future__ import print_function

import logging

from pytest import fixture

from targets.value_meta import ValueMetaConf


def get_value_meta_from_value(name, value, meta_conf):
    from dbnd._core.settings import TrackingConfig
    from dbnd._core.settings.tracking_config import ValueTrackingLevel
    from dbnd._core.settings.tracking_config import get_value_meta

    c = TrackingConfig.current()
    c.value_reporting_strategy = ValueTrackingLevel.ALL
    return get_value_meta(value, meta_conf, tracking_config=c)


logger = logging.getLogger(__name__)


class BaseHistogramTests(object):
    @fixture
    def meta_conf(self):
        return ValueMetaConf.enabled()

    @fixture
    def numbers(self):
        return [1, 1, 3, 1, 5, None, 1, 5, 5, None]

    @fixture
    def numbers_value(self, numbers):
        return self.data_to_value([numbers])

    @fixture
    def floats_value(self, numbers):
        floats = [float(num) if num else None for num in numbers]
        return self.data_to_value([floats])

    def data_to_value(self, data):
        # (List[List]) -> value with ValueType & histogram support (e.g. dataframe)
        """
        Gets a list of lists of values which represents data in table/dataframe format.
        Each list is a column of values.
        """
        raise NotImplementedError()

    def validate_numeric_histogram_and_stats(self, value_meta, column_name):
        """ assuming numbers fixture is used """
        assert column_name in value_meta.histograms
        histogram = value_meta.histograms[column_name]
        assert len(histogram) == 2
        assert len(histogram[0]) == 20
        assert len(histogram[1]) == 21
        assert sum(histogram[0]) == 8

        stats = value_meta.descriptive_stats[column_name]
        assert set(stats.keys()) == {
            "count",
            "mean",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
            "std",
            "type",
            "distinct",
            "null-count",
            "non-null",
        }
        assert stats["count"] == 10
        assert stats["non-null"] == 8
        assert stats["distinct"] == 4
        assert stats["min"] == 1
        assert stats["max"] == 5

    def test_int_column(self, meta_conf, numbers_value):
        value_meta = get_value_meta_from_value("numbers", numbers_value, meta_conf)
        self.validate_numeric_histogram_and_stats(value_meta, "test_column_0")
        return value_meta

    def test_float_column(self, meta_conf, floats_value):
        value_meta = get_value_meta_from_value("floats", floats_value, meta_conf)
        self.validate_numeric_histogram_and_stats(value_meta, "test_column_0")
        return value_meta

    @fixture
    def booleans_value(self):
        booleans = [True] * 10 + [None] * 10 + [False] * 20 + [True] * 20
        return self.data_to_value([booleans])

    def test_boolean_histogram(self, meta_conf, booleans_value):
        value_meta = get_value_meta_from_value("booleans", booleans_value, meta_conf)

        histogram = value_meta.histograms["test_column_0"]
        assert histogram[0] == [30, 20, 10]
        assert histogram[1] == [True, False, None]

        stats = value_meta.descriptive_stats["test_column_0"]
        assert stats["count"] == 60
        assert stats["type"] in ["bool", "boolean"]

    @fixture
    def strings_value(self):
        strings = (
            ["Hello World!"] * 15
            + [None] * 5
            + ["Ola Mundo!"] * 15
            + ["Shalom Olam!"] * 20
            + ["Ola Mundo!"] * 15
        )
        return self.data_to_value([strings])

    def test_strings_histogram(self, meta_conf, strings_value):
        value_meta = get_value_meta_from_value("strings", strings_value, meta_conf)

        histogram = value_meta.histograms["test_column_0"]
        assert histogram[0] == [30, 20, 15, 5]
        assert histogram[1] == ["Ola Mundo!", "Shalom Olam!", "Hello World!", None]

        stats = value_meta.descriptive_stats["test_column_0"]
        assert stats["count"] == 70
        assert stats["non-null"] == 65
        assert stats["null-count"] == 5
        assert stats["distinct"] == 4
        assert stats["type"] in ["str", "string"]

    def test_histogram_others(self, meta_conf):
        strings = []
        for i in range(1, 101):
            str_i = "str-{}".format(i)
            new_strings = [str_i] * i
            strings.extend(new_strings)
        strings_value = self.data_to_value([strings])

        value_meta = get_value_meta_from_value(
            "string_with_others", strings_value, meta_conf
        )

        histogram = value_meta.histograms["test_column_0"]
        assert len(histogram[0]) == 50 and len(histogram[1]) == 50
        assert histogram[0][0] == 100 and histogram[1][0] == "str-100"
        assert histogram[0][10] == 90 and histogram[1][10] == "str-90"
        assert histogram[0][-2] == 52 and histogram[1][-2] == "str-52"
        assert histogram[0][-1] == sum(range(1, 52)) and histogram[1][-1] == "_others"

        stats = value_meta.descriptive_stats["test_column_0"]
        assert stats["count"] == 5050 == sum(histogram[0])
        assert stats["non-null"] == 5050
        assert stats["null-count"] == 0
        assert stats["distinct"] == 100
        assert stats["type"] in ["str", "string"]

    def test_multiple_columns(self, meta_conf, numbers):
        values = [(i, float(i), str(i), str(i)) if i else [None] * 4 for i in numbers]
        values = list(zip(*values))
        df = self.data_to_value(values)
        value_meta = get_value_meta_from_value("multi_column", df, meta_conf)

        self.validate_numeric_histogram_and_stats(value_meta, "test_column_0")
        self.validate_numeric_histogram_and_stats(value_meta, "test_column_1")
        str_histogram_1 = value_meta.histograms["test_column_2"]
        str_histogram_2 = value_meta.histograms["test_column_3"]
        assert str_histogram_1[0] == [4, 3, 2, 1]
        assert str_histogram_1[1] == ["1", "5", None, "3"]
        assert str_histogram_1 == str_histogram_2
