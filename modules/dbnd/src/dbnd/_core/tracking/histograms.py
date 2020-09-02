import typing

from collections import Iterable
from enum import Enum

import attr


if typing.TYPE_CHECKING:
    from typing import Callable, List, Union, Optional
    import pandas as pd
    import pyspark.sql as spark
    from dbnd_postgres.postgres_values import PostgresTable
    from targets.values import ValueType


class HistogramDataType(Enum):
    boolean = "boolean"
    numeric = "numeric"
    string = "string"
    other = "other"


@attr.s
class HistogramSpec(object):
    columns = attr.ib(
        converter=lambda v: frozenset(v) if v else frozenset(), default=frozenset()
    )  # type: Union[set, frozenset]
    only_stats = attr.ib(default=False)  # type: bool
    approx_distinct_count = attr.ib(default=False)  # type: bool
    with_stats = attr.ib(default=False)  # type: bool
    none = attr.ib(default=False)  # type: bool

    @classmethod
    def build_spec(
        cls,
        value_type,  # type: ValueType
        data,  # type: Union[pd.DataFrame, spark.DataFrame, PostgresTable]
        histogram_request,  # type: HistogramRequest
    ):  # type: (...) -> HistogramSpec
        histogram_request = HistogramRequest.from_with_histograms(histogram_request)

        if histogram_request.none:
            return cls(none=True)

        all_columns_map = value_type.get_all_data_columns(data)
        selected_columns = set(histogram_request.include_columns)

        if histogram_request.include_all_numeric:
            selected_columns |= {
                col
                for col, type_ in all_columns_map.items()
                if type_ == HistogramDataType.numeric
            }
        if histogram_request.include_all_boolean:
            selected_columns |= {
                col
                for col, type_ in all_columns_map.items()
                if type_ == HistogramDataType.boolean
            }
        if histogram_request.include_all_string:
            selected_columns |= {
                col
                for col, type_ in all_columns_map.items()
                if type_ == HistogramDataType.string
            }
        selected_columns -= set(histogram_request.exclude_columns)
        return cls(
            columns=selected_columns,
            only_stats=histogram_request.only_stats,
            approx_distinct_count=histogram_request.approx_distinct_count,
            with_stats=histogram_request.with_stats,
        )


def columns_converter(columns):
    # type: (Union[str,Iterable[str], Callable[[], List[str]]]) -> List
    if isinstance(columns, str):
        return columns.split(",")
    elif isinstance(columns, Iterable):
        return [str(col) for col in columns]
    elif callable(columns):
        return columns()
    return []


@attr.s
class HistogramRequest(object):
    """Configuration class for histograms calculations."""

    include_columns = attr.ib(
        converter=columns_converter, factory=list
    )  # type: Union[Callable[[], List[str]], List]
    exclude_columns = attr.ib(
        converter=columns_converter, factory=list
    )  # type: Union[Callable[[], List[str]], List]
    include_all_boolean = attr.ib(default=False)  # type: bool
    include_all_numeric = attr.ib(default=False)  # type: bool
    include_all_string = attr.ib(default=False)  # type: bool
    only_stats = attr.ib(default=False)  # type: bool
    with_stats = attr.ib(default=False)  # type: bool
    approx_distinct_count = attr.ib(default=False)  # type: bool
    none = attr.ib(default=False)  # type: bool

    # TODO: Make these helpers class properties
    @classmethod
    def ALL_BOOLEAN(cls):
        """Calculate statistics and histograms for all columns of boolean type(s)"""
        return cls(include_all_boolean=True)

    @classmethod
    def ALL_NUMERIC(cls):
        """Calculate statistics and histograms for all columns of Numeric type(s)"""
        return cls(include_all_numeric=True)

    @classmethod
    def ALL_STRING(cls):
        """Calculate statistics and histograms for all columns of String type(s). Usually this means all non boolean, non numeric types"""
        return cls(include_all_string=True)

    @classmethod
    def ALL(cls):
        """Calculate statistics and histograms for all columns"""
        return cls(
            include_all_boolean=True,
            include_all_numeric=True,
            include_all_string=True,
            with_stats=True,
        )

    @classmethod
    def DEFAULT(cls):
        """Calculate histograms for all columns"""
        return cls(
            include_all_boolean=True, include_all_numeric=True, include_all_string=True
        )

    @classmethod
    def NONE(cls):
        """Don't calculate statistics and histograms"""
        return cls(none=True)

    @classmethod
    def ONLY_STATS(cls):
        """Calculate statistics, but no histograms. Can save some CPU time and provide high level data overview"""
        return cls(only_stats=True)

    @classmethod
    def from_with_histograms(cls, with_histograms):
        # type: (Optional[Union[bool, str, List[str], HistogramRequest]]) -> HistogramRequest
        histogram_request = HistogramRequest.NONE()
        if isinstance(with_histograms, HistogramRequest):
            histogram_request = with_histograms
        elif isinstance(with_histograms, bool):
            histogram_request = (
                HistogramRequest.DEFAULT()
                if with_histograms
                else HistogramRequest.NONE()
            )
        elif isinstance(with_histograms, (list, str)):
            histogram_request = HistogramRequest(include_columns=with_histograms)

        return histogram_request
