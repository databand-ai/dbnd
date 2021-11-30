from typing import List, Optional

import attr

from more_itertools import first

from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor.marshmallow import fields


def get_column_stats_by_col_name(
    columns_stats: List["ColumnStatsArgs"], column_name: str,
) -> Optional["ColumnStatsArgs"]:
    # Returns column_stats if column_name is found in columns_stats list
    return first(
        filter(lambda col_stats: col_stats.column_name == column_name, columns_stats,),
        None,
    )


@attr.s(auto_attribs=True)
class ColumnStatsArgs:
    column_name: str = attr.ib()
    column_type: str = attr.ib()

    records_count: int = attr.ib(default=None)
    distinct_count: int = attr.ib(default=None)
    null_count: int = attr.ib(default=None)

    # Metric for non-numeric column type
    unique_count: int = attr.ib(default=None)
    # Most frequent value
    top_value: any = attr.ib(default=None)
    top_freq_count: int = attr.ib(default=None)

    # Metric for numeric column type
    mean_value: float = attr.ib(default=None)
    min_value: float = attr.ib(default=None)
    max_value: float = attr.ib(default=None)
    std_value: float = attr.ib(default=None)
    # Percentiles
    quartile_1: float = attr.ib(default=None)
    quartile_2: float = attr.ib(default=None)
    quartile_3: float = attr.ib(default=None)

    @property
    def non_null_count(self) -> Optional[int]:
        if self.records_count is None or self.null_count is None:
            return None
        return self.records_count - self.null_count

    def dump_to_stats_dict(self) -> dict:
        # Returns legacy stats dict for backward compatability support
        stats = {
            "type": self.column_type,
            "count": self.records_count,
            "distinct": self.distinct_count,
            "null-count": self.null_count,
            "non-null": self.non_null_count,
            "top": self.top_value,
            "freq": self.top_freq_count,
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

    def as_dict(self):
        return attr.asdict(self, filter=lambda _, value: value is not None)


class ColumnStatsSchema(ApiStrictSchema):
    column_name = fields.String(required=True)
    column_type = fields.String(required=True)

    records_count = fields.Integer(allow_none=True)
    distinct_count = fields.Integer(allow_none=True)
    null_count = fields.Integer(allow_none=True)
    non_null_count = fields.Integer(allow_none=True)

    # Metric for non-numeric column type
    unique_count = fields.Integer(allow_none=True)
    # Most frequent value
    top_value = fields.Raw(allow_none=True)
    top_freq_count = fields.Integer(allow_none=True)

    # Metric for numeric column type
    mean_value = fields.Float(allow_none=True)
    min_value = fields.Float(allow_none=True)
    max_value = fields.Float(allow_none=True)
    std_value = fields.Float(allow_none=True)
    # Percentiles
    quartile_1 = fields.Float(allow_none=True)
    quartile_2 = fields.Float(allow_none=True)
    quartile_3 = fields.Float(allow_none=True)
