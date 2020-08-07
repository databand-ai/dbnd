import pytest

from dbnd._core.tracking.histograms import HistogramRequest, HistogramSpec
from targets.values import get_value_type_of_obj


class TestHistogramSpec(object):
    @pytest.mark.parametrize(
        "histogram_request,expected_columns,expected_only_stats,expected_none",
        [
            (HistogramRequest.ALL(), ["Names", "Births", "Married"], False, False),
            (HistogramRequest.DEFAULT(), ["Names", "Births", "Married"], False, False),
            (HistogramRequest.ALL_BOOLEAN(), ["Married"], False, False),
            (HistogramRequest.ALL_NUMERIC(), ["Births"], False, False),
            (HistogramRequest.ALL_STRING(), ["Names"], False, False),
            (HistogramRequest.ONLY_STATS(), [], True, False),
            (HistogramRequest.NONE(), [], False, True),
            (
                HistogramRequest(include_columns=["Names", "Married"]),
                ["Names", "Married"],
                False,
                False,
            ),
            (
                HistogramRequest(include_columns=lambda: ["Names", "Married"]),
                ["Names", "Married"],
                False,
                False,
            ),
            (
                HistogramRequest(
                    include_columns=["Names", "Births", "Married"],
                    exclude_columns=["Names"],
                ),
                ["Births", "Married"],
                False,
                False,
            ),
            (
                HistogramRequest(
                    include_columns=lambda: ["Names", "Births", "Married"],
                    exclude_columns=lambda: ["Names"],
                ),
                ["Births", "Married"],
                False,
                False,
            ),
        ],
    )
    def test_histogram_build_spec_columns(
        self,
        pandas_data_frame,
        histogram_request,
        expected_columns,
        expected_only_stats,
        expected_none,
    ):
        # Arrange
        value_type = get_value_type_of_obj(pandas_data_frame)

        # Act
        histogram_spec = HistogramSpec.build_spec(
            value_type, pandas_data_frame, histogram_request
        )

        # Assert
        assert histogram_spec.columns == frozenset(expected_columns)
        assert histogram_spec.only_stats == expected_only_stats
        assert histogram_spec.none == expected_none
