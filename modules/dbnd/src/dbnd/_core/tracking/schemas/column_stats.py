from typing import Any, Callable, List, Optional

import attr

from more_itertools import first

from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor.marshmallow import fields, post_dump, post_load


def get_column_stats_by_col_name(
    columns_stats: List["ColumnStatsArgs"], column_name: str
) -> Optional["ColumnStatsArgs"]:
    # Returns column_stats if column_name is found in columns_stats list
    return first(
        filter(lambda col_stats: col_stats.column_name == column_name, columns_stats),
        None,
    )


@attr.s(auto_attribs=True)
class ColumnStatsArgs:
    column_name: str = attr.ib()
    column_type: str = attr.ib()

    records_count: int = attr.ib()
    distinct_count: Optional[int] = attr.ib(default=None)
    null_count: Optional[int] = attr.ib(default=None)

    # Metric for non-numeric column type
    unique_count: Optional[int] = attr.ib(default=None)
    # Most frequent value
    most_freq_value: Optional[Any] = attr.ib(default=None)
    most_freq_value_count: Optional[int] = attr.ib(default=None)

    # Metric for numeric column type
    mean_value: Optional[float] = attr.ib(default=None)
    min_value: Optional[float] = attr.ib(default=None)
    max_value: Optional[float] = attr.ib(default=None)
    std_value: Optional[float] = attr.ib(default=None)
    # Percentiles
    quartile_1: Optional[float] = attr.ib(default=None)
    quartile_2: Optional[float] = attr.ib(default=None)
    quartile_3: Optional[float] = attr.ib(default=None)

    non_null_count: Optional[int] = attr.ib()
    null_percent: Optional[float] = attr.ib()

    @non_null_count.default
    def _non_null_count(self) -> Optional[int]:
        if self.records_count is None or self.null_count is None:
            return None
        return self.records_count - self.null_count

    @null_percent.default
    def _null_percent(self) -> Optional[float]:
        if self.null_count is None or self.records_count == 0:
            return None

        return (self.null_count / self.records_count) * 100

    def dump_to_legacy_stats_dict(self) -> dict:
        # Returns legacy stats dict for backward compatability support with dbnd-tracking
        stats = {
            "type": self.column_type,
            "count": self.records_count,
            "distinct": self.distinct_count,
            "null-count": self.null_count,
            "non-null": self.non_null_count,
            "top": self.most_freq_value,
            "freq": self.most_freq_value_count,
            "unique": self.unique_count,
            "mean": self.mean_value,
            "min": self.min_value,
            "max": self.max_value,
            "std": self.std_value,
            "25%": self.quartile_1,
            "50%": self.quartile_2,
            "75%": self.quartile_3,
        }
        filtered_stats = {k: v for k, v in stats.items() if v is not None}
        return {self.column_name: filtered_stats}

    def get_stats(self, stats_filter: Optional[Callable] = None) -> dict:
        """Get the column's stats filtered by given filter or by a non-null values filter

        Args:
            stats_filter: Callable that takes two arguments: the stat's name and the stat's value.
            The return code determines whether a stat is included (``True``) or dropped (``False``).
            Defaults to non-null values filter.

        Returns:
            dict: Filtered stats dict
        """
        stats = {
            "distinct_count": self.distinct_count,
            "null_count": self.null_count,
            "unique_count": self.unique_count,
            "most_freq_value": self.most_freq_value,
            "most_freq_value_count": self.most_freq_value_count,
            "mean_value": self.mean_value,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "std_value": self.std_value,
            "quartile_1": self.quartile_1,
            "quartile_2": self.quartile_2,
            "quartile_3": self.quartile_3,
            "non_null_count": self.non_null_count,
            "null_percent": self.null_percent,
        }
        stats_filter = stats_filter or (lambda _, value: value is not None)
        return {k: v for k, v in stats.items() if stats_filter(k, v)}

    def get_numeric_stats(self) -> dict:
        """Get all the column's numeric stats
        Returns:
            dict: Filter stats dict by numeric values
        """

        def _numberic_stats_filter(_, value) -> bool:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return True
            return False

        return self.get_stats(stats_filter=_numberic_stats_filter)

    def as_dict(self) -> dict:
        return attr.asdict(self, filter=lambda _, value: value is not None)


class ColumnStatsSchema(ApiStrictSchema):
    column_name = fields.String(required=True)
    column_type = fields.String(required=True)

    records_count = fields.Integer(required=True)
    distinct_count = fields.Integer(required=False, allow_none=True)
    null_count = fields.Integer(required=False, allow_none=True)

    # Metric for non-numeric column type
    unique_count = fields.Integer(required=False, allow_none=True)
    # Most frequent value - load_from attrs to support backward compatibility with SDK < 0.59.0
    most_freq_value = fields.Raw(required=False, allow_none=True, load_from="top_value")
    most_freq_value_count = fields.Integer(
        required=False, allow_none=True, load_from="top_freq_count"
    )

    # Metric for numeric column type
    mean_value = fields.Float(required=False, allow_none=True)
    min_value = fields.Float(required=False, allow_none=True)
    max_value = fields.Float(required=False, allow_none=True)
    std_value = fields.Float(required=False, allow_none=True)
    # Percentiles
    quartile_1 = fields.Float(required=False, allow_none=True)
    quartile_2 = fields.Float(required=False, allow_none=True)
    quartile_3 = fields.Float(required=False, allow_none=True)

    # Hybrid properties - We only dump them so we don't load them back to ColumnStatsArgs when we make_object
    # We don't save them in DB, and they are only computed in ColumnStatsArgs
    non_null_count = fields.Integer(required=False, allow_none=True, dump_only=True)
    null_percent = fields.Float(required=False, allow_none=True, dump_only=True)

    @post_load
    def make_object(self, data: dict) -> ColumnStatsArgs:
        return ColumnStatsArgs(**data)

    @post_dump
    def dump_object(self, data: dict) -> dict:
        return {k: v for k, v in data.items() if v is not None}
