from typing import List, Optional

import attr

from more_itertools import first

from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._core.utils.dotdict import build_dict_from_instance_properties
from dbnd._vendor.marshmallow import fields, post_dump, post_load


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

    records_count: int = attr.ib()
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

    @property
    def null_percent(self) -> Optional[float]:
        if self.null_count is None:
            return None

        return (self.null_count / self.records_count) * 100

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

    def as_dict(self, with_property_methods: bool = True) -> dict:
        if with_property_methods:
            attr_dict = build_dict_from_instance_properties(self)
        else:
            attr_dict = attr.asdict(self)
        return {k: v for k, v in attr_dict.items() if v is not None}


class ColumnStatsSchema(ApiStrictSchema):
    column_name = fields.String(required=True)
    column_type = fields.String(required=True)

    records_count = fields.Integer(required=True)
    distinct_count = fields.Integer(required=False)
    null_count = fields.Integer(required=False)

    # Metric for non-numeric column type
    unique_count = fields.Integer(required=False)
    # Most frequent value
    top_value = fields.Raw(required=False)
    top_freq_count = fields.Integer(required=False)

    # Metric for numeric column type
    mean_value = fields.Float(required=False)
    min_value = fields.Float(required=False)
    max_value = fields.Float(required=False)
    std_value = fields.Float(required=False)
    # Percentiles
    quartile_1 = fields.Float(required=False)
    quartile_2 = fields.Float(required=False)
    quartile_3 = fields.Float(required=False)

    # Hybrid properties - We only dump them so we don't load them back to ColumnStatsArgs when we make_object
    # We don't save them in DB, and they are only computed in ColumnStatsArgs
    non_null_count = fields.Integer(required=False, dump_only=True)
    null_percent = fields.Float(required=False, dump_only=True)

    @post_load
    def make_object(self, data: dict) -> ColumnStatsArgs:
        return ColumnStatsArgs(**data)

    @post_dump
    def dump_object(self, data: dict) -> dict:
        return {k: v for k, v in data.items() if v is not None}
