# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.tracking.schemas.column_stats import ColumnStatsArgs
from targets.data_schema import DataSchemaArgs
from targets.value_meta import ValueMeta


class TestValueMeta(object):
    def test_blank_value_meta(self):
        meta = ValueMeta()

        assert isinstance(meta, ValueMeta)
        assert meta.value_preview is None
        assert meta.data_dimensions is None
        assert meta.query is None
        assert meta.data_schema is None
        assert meta.data_hash is None
        assert meta.columns_stats == []
        assert meta.histograms is None
        assert meta.histogram_system_metrics is None
        assert meta.op_source is None

    def test_blank_basic_value_meta(self):
        meta = ValueMeta.basic([], records_count=0)

        assert meta is None

    def test_basic_value_meta(self):
        expected_column_stats = [
            ColumnStatsArgs(
                column_name="default",
                column_type="default",
                records_count=1,
                distinct_count=None,
                null_count=None,
                unique_count=None,
                most_freq_value=None,
                most_freq_value_count=None,
                mean_value=None,
                min_value=None,
                max_value=None,
                std_value=None,
                quartile_1=None,
                quartile_2=None,
                quartile_3=None,
                non_null_count=None,
                null_percent=None,
            )
        ]
        expected_data_schema = DataSchemaArgs(
            type=None,
            columns_names=["default"],
            columns_types=None,
            shape=(1, 1),
            byte_size=None,
        )

        meta = ValueMeta.basic(expected_column_stats, records_count=1)

        assert isinstance(meta, ValueMeta)
        assert meta.value_preview is None
        assert meta.data_dimensions == (1, 1)
        assert meta.query is None
        assert meta.data_schema == expected_data_schema
        assert meta.data_hash is None
        assert meta.columns_stats == expected_column_stats
        assert meta.histograms is None
        assert meta.histogram_system_metrics is None
        assert meta.op_source is None

    def test_basic_customized_value_meta(self):

        columns = [
            ColumnStatsArgs(
                column_name="test1", column_type="type_test", std_value=5.77
            ),
            ColumnStatsArgs(
                column_name="test2", column_type="type_test", mean_value=13.0
            ),
            ColumnStatsArgs(
                column_name="test3", column_type="type_test", min_value=32.33
            ),
            ColumnStatsArgs(
                column_name="test4", column_type="type_test", max_value=66.0
            ),
        ]

        expected_data_schema = DataSchemaArgs(
            type=None,
            columns_names=["test1", "test2", "test3", "test4"],
            columns_types=None,
            shape=(5, 4),
            byte_size=None,
        )

        meta = ValueMeta.basic(columns, records_count=5)

        assert isinstance(meta, ValueMeta)
        assert meta.value_preview is None
        assert meta.data_dimensions == (5, 4)
        assert meta.query is None
        assert meta.data_schema == expected_data_schema
        assert meta.data_hash is None
        assert meta.columns_stats == columns
        assert meta.histograms is None
        assert meta.histogram_system_metrics is None
        assert meta.op_source is None

    def test_full_customized_value_meta(self):
        given_dict_column_stats = {
            "column_name": "1X",  # str -> must match one of column_names from data_schema
            "column_type": "X",  # optional str
            "records_count": 9,  # optional int
            "distinct_count": 9,  # optional int
            "null_count": 9,  # optional int
            "unique_count": 9,  # optional int
            "most_freq_value": 9,  # optional int
            "most_freq_value_count": 9,  # optional int
            "mean_value": 9.0,  # optional float
            "min_value": 9.0,  # optional float
            "max_value": 9.0,  # optional float
            "std_value": 9.0,  # optional float
            "quartile_1": 9.0,  # optional float
            "quartile_2": 9.0,  # optional float
            "quartile_3": 9.0,  # optional float
            "non_null_count": 9,  # optional int
            "null_percent": 9.0,  # optional float
        }

        given_dict_data_schema = {
            "type": "X",  # optional str
            "columns_names": [
                given_dict_column_stats["column_name"]
            ],  # optional List[str,...]
            "columns_types": {"A": "xyz", "B": "GHJ"},  # optional Dict['str':'str',...]
            "shape": (2, 2),  # optional Tuple(int,int)
            "byte_size": 4,  # optional int
        }

        expected_general_metrics = {
            "value_preview": "X",  # str
            "data_dimensions": (9, 9),
            # Optional[Tuple[Optional[int], Optional[int]]] -> if not provided data schema insights not available
            "query": "X",  # Optional[str]
            "data_hash": "X",  # Optional[str]
            "histograms": {"k1X": (9, 9)},  # Optional[Dict[str, Tuple]]
            "histogram_system_metrics": {"k1X": "v1X"},  # Optional[Dict]
            "op_source": "X",  # Optional[str]
        }

        meta = ValueMeta(
            value_preview=expected_general_metrics["value_preview"],
            data_dimensions=expected_general_metrics["data_dimensions"],
            query=expected_general_metrics["query"],
            data_hash=expected_general_metrics["data_hash"],
            histograms=expected_general_metrics["histograms"],
            histogram_system_metrics=expected_general_metrics[
                "histogram_system_metrics"
            ],
            op_source=expected_general_metrics["op_source"],
            data_schema=DataSchemaArgs(**given_dict_data_schema),
        )

        meta.add_column_stats(given_dict_column_stats)

        expected_column_stats = [
            ColumnStatsArgs(
                column_name="1X",
                column_type="X",
                records_count=9,
                distinct_count=9,
                null_count=9,
                unique_count=9,
                most_freq_value=9,
                most_freq_value_count=9,
                mean_value=9.0,
                min_value=9.0,
                max_value=9.0,
                std_value=9.0,
                quartile_1=9.0,
                quartile_2=9.0,
                quartile_3=9.0,
                non_null_count=9,
                null_percent=9.0,
            )
        ]
        expected_data_schema = DataSchemaArgs(
            type="X",
            columns_names=["1X", "1X"],
            columns_types={"A": "xyz", "B": "GHJ"},
            shape=(2, 2),
            byte_size=4,
        )

        assert isinstance(meta, ValueMeta)
        assert meta.value_preview == expected_general_metrics["value_preview"]
        assert meta.data_dimensions == expected_general_metrics["data_dimensions"]
        assert meta.query == expected_general_metrics["query"]
        assert meta.data_schema == expected_data_schema
        assert meta.data_hash == expected_general_metrics["data_hash"]
        assert meta.columns_stats == expected_column_stats
        assert meta.histograms == expected_general_metrics["histograms"]
        assert (
            meta.histogram_system_metrics
            == expected_general_metrics["histogram_system_metrics"]
        )
        assert meta.op_source == expected_general_metrics["op_source"]

    @pytest.mark.parametrize(
        "params",
        [
            {
                "column_name": (2, 3),
                "column_type": "type_test",
                "test_property": {"records_count": 4},
            },
            {
                "column_name": "test1",
                "column_type": (2, 3),
                "test_property": {"records_count": 4},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"records_count": 5.5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"distinct_count": 5.5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"null_count": 5.5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"unique_count": 5.5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"mean_value": 5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"min_value": 5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"max_value": 5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"std_value": 5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"quartile_1": 5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"quartile_2": 5},
            },
            {
                "column_name": "test1",
                "column_type": "type_test",
                "test_property": {"quartile_3": 5},
            },
        ],
    )
    def test_basic_bad_parameters_value_meta(self, params):
        with pytest.raises(TypeError) as e:
            test_property_key, test_property_value = list(
                params["test_property"].items()
            )[0]
            columns = [
                ColumnStatsArgs(
                    column_name=params["column_name"],
                    column_type=params["column_type"],
                    **{test_property_key: test_property_value}
                )
            ]
            ValueMeta.basic(columns)
        assert e.type == TypeError
