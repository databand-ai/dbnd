import pandas as pd

from targets.value_meta import ValueMetaConf
from targets.values.pandas_histograms import PandasHistograms


# fmt: off
diverse_df = pd.DataFrame({
    'int_column': [6, 7, None, 1, 9, None, 3, 7, 5, 1, 1, 6, 7, 3, 7, 4, 5, 4, 3, 7, 3,
                   2, None, 6, 6, 2, 4, None, 7, 2, 2, 6, 9, 6, 1, 9, 2, 4, 0, 5, 3, 8,
                   9, 6, 7, 5, None, 1, 1, 2, None, 5, 6, 8, 6, 9, 1, 9, 5, 9, 6, 5, 6,
                   8, 9, 1, 9, 4, None, 3, 1, 6, 1, 4, 9, 3, 1, 2, None, 7, 3, 1, 9, 2,
                   4, 5, 2, 8, 7, 8, 1, 7, 7, 6, 3, 0, 6, 8, 6, 9],
    'float_column': [9.0, 4.0, 6.0, 6.0, 7.0, 2.0, 5.0, 1.0, 8.0, 4.0, 3.0, 4.0, 2.0,
                     7.0, 3.0, 9.0, 7.0, 5.0, 3.0, 9.0, 4.0, 9.0, None, 5.0, 5.0, 2.0,
                     4.0, 4.0, 7.0, 5.0, 1.0, 8.0, 7.0, 4.0, 1.0, 0.0, 6.0, 2.0, 1.0,
                     2.0, 7.0, 3.0, 0.0, 8.0, 3.0, 2.0, None, 0.0, 8.0, None, 9.0, 2.0,
                     2.0, 9.0, 1.0, 6.0, 6.0, 1.0, 0.0, 8.0, 7.0, 9.0, 2.0, 9.0, 9.0,
                     2.0, 0.0, 7.0, 5.0, 7.0, 3.0, 5.0, 1.0, 2.0, 4.0, 3.0, 1.0, 0.0,
                     3.0, 1.0, 4.0, 8.0, 2.0, None, 2.0, 9.0, 7.0, 7.0, 8.0, 5.0, 7.0,
                     None, 7.0, 4.0, 8.0, 7.0, 9.0, 7.0, 6.0, None],
    'bool_column': [None, True, None, True, None, None, None, True, True, None, None,
                    True, None, True, None, None, False, False, None, False, None,
                    True, False, False, True, None, True, None, False, False, None,
                    True, False, True, None, None, None, None, None, True, True, None,
                    None, None, False, None, True, None, True, False, True, True,
                    False, False, None, False, False, True, True, None, None, True,
                    True, True, False, None, False, True, False, False, False, None,
                    False, False, None, True, True, False, None, True, False, False,
                    True, True, False, None, None, True, False, False, False, False,
                    False, True, False, False, None, False, True, True],
    'str_column': ['baz', 'baz', 'bar', None, '', '', 'baz', 'foo', None, '', 'bar',
                   None, 'bar', 'baz', '', None, 'foo', None, 'bar', None, 'bar',
                   'bar', '', None, 'foo', '', 'bar', 'foo', 'baz', None, '', 'bar',
                   'foo', 'foo', 'foo', 'foo', 'bar', None, None, 'foo', '', '', '',
                   'bar', 'foo', '', 'bar', '', '', 'baz', 'baz', 'bar', 'baz', 'baz',
                   None, '', 'foo', '', None, 'baz', 'baz', 'baz', 'foo', 'foo', 'baz',
                   None, 'foo', None, 'foo', None, 'bar', None, 'bar', 'baz', 'foo',
                   'foo', None, 'foo', '', 'baz', 'baz', 'baz', None, 'bar', None,
                   None, 'bar', '', 'foo', 'baz', 'baz', '', 'foo', 'baz', 'foo', '',
                   'bar', None, 'foo', '']
})
# fmt: on


def test_pandas_v0_histograms():
    # Tests pandas histograms calculation is stable across Pandas v1 & v0
    meta_conf = ValueMetaConf.enabled()
    stats, histograms = PandasHistograms(
        diverse_df, meta_conf
    ).get_histograms_and_stats()

    # fmt: off
    assert stats == {
        "bool_column": {
            "count": 100,
            "distinct": 3,
            "freq": 33,
            "non-null": 65,
            "null-count": 35,
            "top": False,
            "type": "bool",
            "unique": 2,
        },
        "float_column": {
            "25%": 2.0,
            "50%": 5.0,
            "75%": 7.0,
            "count": 100,
            "distinct": 11,
            "max": 9.0,
            "mean": 4.7127659574,
            "min": 0.0,
            "non-null": 94,
            "null-count": 6,
            "std": 2.8572576537,
            "type": "float64",
        },
        "int_column": {
            "25%": 2.0,
            "50%": 5.0,
            "75%": 7.0,
            "count": 100,
            "distinct": 11,
            "max": 9.0,
            "mean": 4.8804347826,
            "min": 0.0,
            "non-null": 92,
            "null-count": 8,
            "std": 2.7449950111,
            "type": "float64",
        },
        "str_column": {
            "count": 100,
            "distinct": 5,
            "freq": 22,
            "non-null": 79,
            "null-count": 21,
            "top": "foo",
            "type": "str",
            "unique": 4,
        },
    }
    # "str_column" calculation is unstable hence these unpacked assertions
    assert set(histograms.keys()) == {"bool_column", "float_column", "int_column", "str_column"}
    assert histograms["bool_column"] == [[35, 33, 32], [None, False, True]]
    assert histograms["float_column"] == [
        [6, 0, 9, 0, 13, 0, 8, 0, 10, 0, 0, 8, 0, 6, 0, 15, 0, 8, 0, 11],
        [0.0, 0.45, 0.9, 1.35, 1.8, 2.25, 2.7, 3.15, 3.6, 4.05, 4.5, 4.95, 5.4,
         5.8500000000000005, 6.3, 6.75, 7.2, 7.65, 8.1, 8.55, 9.0]
    ]
    assert histograms["int_column"] == [
        [2, 0, 13, 0, 9, 0, 9, 0, 7, 0, 0, 8, 0, 15, 0, 11, 0, 6, 0, 12],
        [0.0, 0.45, 0.9, 1.35, 1.8, 2.25, 2.7, 3.15, 3.6, 4.05, 4.5, 4.95, 5.4,
         5.8500000000000005, 6.3, 6.75, 7.2, 7.65, 8.1, 8.55, 9.0]
    ]
    assert histograms["str_column"][0] == [22, 21, 20, 20, 17]
    # "str_column" calculation is unstable
    assert set(histograms["str_column"][1]) == {"foo", None, "", "baz", "bar"}
    # fmt: on


def test_pandas_histograms_work_with_NaNs(pandas_data_frame):
    pandas_data_frame = pandas_data_frame.drop(columns="Names").append([{"foo": 42}])
    meta_conf = ValueMetaConf.enabled()
    stats, histograms = PandasHistograms(
        pandas_data_frame, meta_conf
    ).get_histograms_and_stats()

    assert sorted(stats.keys()) == sorted(["Births", "foo"])  # noqa
    assert sorted(histograms.keys()) == sorted(["Births", "foo"])  # noqa
    assert stats == {
        "Births": {
            "25%": 155.0,
            "50%": 578.0,
            "75%": 968.0,
            "count": 6,
            "distinct": 6,
            "max": 973.0,
            "mean": 550.2,
            "min": 77.0,
            "non-null": 5,
            "null-count": 1,
            "std": 428.4246724921,
            "type": "float64",
        },
        "foo": {
            "25%": 42.0,
            "50%": 42.0,
            "75%": 42.0,
            "count": 6,
            "distinct": 2,
            "max": 42.0,
            "mean": 42.0,
            "min": 42.0,
            "non-null": 1,
            "null-count": 5,
            "type": "float64",
        },
    }
    # fmt: off
    assert histograms == {
        "Births": [
            [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
            [77.0, 121.8, 166.6, 211.39999999999998, 256.2, 301.0, 345.79999999999995,
             390.59999999999997, 435.4, 480.2, 525.0, 569.8, 614.5999999999999, 659.4,
             704.1999999999999, 749.0, 793.8, 838.5999999999999, 883.4,
             928.1999999999999, 973.0],
        ],
        "foo": [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [41.5, 41.55, 41.6, 41.65, 41.7, 41.75, 41.8, 41.85, 41.9, 41.95, 42.0,
             42.05, 42.1, 42.15, 42.2, 42.25, 42.3, 42.35, 42.4, 42.45, 42.5]
        ]
    }
    # fmt: on
