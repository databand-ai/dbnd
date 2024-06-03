# © Copyright Databand.ai, an IBM Company 2022

from collections import ChainMap
from typing import Dict, List, Optional, Tuple, Union

import attr
import six

from dbnd._core.constants import MetricSource
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.tracking.schemas.column_stats import (
    ColumnStatsArgs,
    get_column_stats_by_col_name,
)
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.utils.timezone import utcnow
from targets.data_schema import DataSchemaArgs, load_data_schema


# keep it below VALUE_PREVIEW_MAX_LEN at web
_DEFAULT_VALUE_PREVIEW_MAX_LEN = 10000


@attr.s(slots=True)
class ValueMeta(object):
    value_preview: Optional[str] = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    data_dimensions: Optional[Tuple[Optional[int], Optional[int]]] = attr.ib(
        default=None,
        validator=attr.validators.optional(
            attr.validators.deep_iterable(
                member_validator=attr.validators.optional(
                    attr.validators.instance_of(int)
                ),
                iterable_validator=attr.validators.instance_of(tuple),
            )
        ),
    )
    query: Optional[str] = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    data_schema: Optional[Union["DataSchemaArgs", dict]] = attr.ib(
        default=None, converter=load_data_schema
    )
    data_hash: Optional[str] = attr.ib(default=None)
    columns_stats: List["ColumnStatsArgs"] = attr.ib(default=attr.Factory(list))
    histograms: Optional[Dict[str, Tuple]] = attr.ib(default=None)
    histogram_system_metrics: Optional[Dict] = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(dict)),
    )
    op_source: Optional[str] = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )

    @classmethod
    def basic(cls, columns_list: List[ColumnStatsArgs], records_count: int):
        if len(columns_list) < 1:
            return None

        all_columns_count = len(columns_list)
        data_shape = (records_count, all_columns_count)

        for col in columns_list:
            col.records_count = records_count

        schema = DataSchemaArgs(
            columns_names=[col.column_name for col in columns_list], shape=data_shape
        )

        return ValueMeta(
            data_schema=schema, columns_stats=columns_list, data_dimensions=data_shape
        )

    def add_column_stats(self, column_data) -> None:
        column_stats_args = ColumnStatsArgs(**column_data)
        if self.data_schema and column_data["column_name"]:
            self.data_schema.columns_names.append(column_data["column_name"])
        self.columns_stats.append(column_stats_args)

    def _build_metrics_for_key(self, key, meta_conf=None):
        # type: (str, Optional[ValueMetaConf]) -> Dict[str, List[Metric]]
        ts = utcnow()
        dataframe_metric_value = {}
        data_metrics, hist_metrics = [], []
        metric_source = MetricSource.user
        if self.data_dimensions:
            dataframe_metric_value["data_dimensions"] = self.data_dimensions
            for dim, size in enumerate(self.data_dimensions):
                key_name = f"{key}.shape{dim}"
                self._append_metric(data_metrics, key_name, metric_source, size, ts)
                name = "rows" if dim == 0 else "columns"
                key_name = f"{key}.{name}"
                self._append_metric(data_metrics, key_name, metric_source, size, ts)

        if meta_conf and meta_conf.log_schema:
            data_schema = self.data_schema.as_dict() if self.data_schema else None
            dataframe_metric_value["schema"] = data_schema
            key_name = f"{key}.schema"
            self._append_metric(
                data_metrics, key_name, metric_source, None, ts, data_schema
            )

        if meta_conf and meta_conf.log_preview:
            dataframe_metric_value["value_preview"] = self.value_preview
            dataframe_metric_value["type"] = "dataframe_metric"
            key_name = str(key)
            self._append_metric(
                data_metrics, key_name, metric_source, None, ts, dataframe_metric_value
            )

        metric_source = MetricSource.histograms
        if self.histogram_system_metrics:
            key_name = f"{key}.histogram_system_metrics"
            self._append_metric(
                hist_metrics,
                key_name,
                metric_source,
                None,
                ts,
                self.histogram_system_metrics,
            )

        if self.histograms:
            key_name = f"{key}.histograms"
            self._append_metric(
                hist_metrics, key_name, metric_source, None, ts, self.histograms
            )

        if self.columns_stats:
            # We dump_op_column_stats to old stats_dict for backward compatibility support
            stats_dict = self._get_stats_dict_from_columns_stats()
            key_name = f"{key}.stats"
            self._append_metric(
                hist_metrics, key_name, metric_source, None, ts, stats_dict
            )
            for col_name, stats in stats_dict.items():
                for stat, value in stats.items():
                    key_name = f"{key}.{col_name}.{stat}"
                    self._append_metric(
                        hist_metrics, key_name, metric_source, value, ts
                    )
        return {"user": data_metrics, "histograms": hist_metrics}

    def _get_column_stats_by_col_name(
        self, column_name: str
    ) -> Optional[ColumnStatsArgs]:
        return get_column_stats_by_col_name(self.columns_stats, column_name)

    def _get_stats_dict_from_columns_stats(self) -> Optional[dict]:
        # Returns legacy stats dict for backward compatability support
        return dict(
            ChainMap(
                *[
                    col_stats.dump_to_legacy_stats_dict()
                    for col_stats in self.columns_stats
                ]
            )
        )

    def _append_metric(
        self, data_metrics, key_name, metric_source, value, ts, value_json=None
    ):
        data_metrics.append(
            Metric(
                key=key_name,
                value=value,
                source=metric_source,
                timestamp=ts,
                value_json=value_json,
            )
        )


@attr.s
class ValueMetaConf(object):
    log_preview = attr.ib(default=None)  # type: Optional[bool]
    log_preview_size = attr.ib(default=None)  # type: Optional[int]
    log_schema = attr.ib(default=None)  # type: Optional[bool]
    log_size = attr.ib(default=None)  # type: Optional[bool]
    log_histograms = attr.ib(
        default=None, converter=LogDataRequest.from_user_param
    )  # type: Optional[Union[LogDataRequest, bool]]
    log_stats = attr.ib(
        default=None, converter=LogDataRequest.from_user_param
    )  # type: Optional[Union[LogDataRequest, bool]]

    def get_preview_size(self):
        return self.log_preview_size or _DEFAULT_VALUE_PREVIEW_MAX_LEN

    @classmethod
    def enabled(cls):
        return ValueMetaConf(
            log_size=True,
            log_preview=True,
            log_schema=True,
            log_stats=True,
            log_histograms=True,
        )

    @classmethod
    def disabled_expensive(cls):
        """
        Disabling any config that can be expensive to calculate
        """
        return ValueMetaConf(
            log_preview=False, log_histograms=False, log_stats=False, log_size=False
        )

    def merge_if_none(self, other):
        # type: (ValueMetaConf, ValueMetaConf) -> ValueMetaConf
        """
        Merging the current meta config with the `other` by merging if None strategy.
        which means - take they value from `other` only if the value in self is none.
        """
        if not isinstance(other, ValueMetaConf):
            raise ValueError(
                "Expected ValueMetaConf got instead {other_type}".format(
                    other_type=type(other)
                )
            )

        # collecting all the values from `other`, only if they are None in `self`
        to_merge = {
            key: value
            for key, value in six.iteritems(other.__dict__)
            if self.__dict__[key] is None
        }
        return attr.evolve(self, **to_merge)
