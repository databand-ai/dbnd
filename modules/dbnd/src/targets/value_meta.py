import typing

import attr
import six

from dbnd._core.constants import MetricSource
from dbnd._core.tracking.log_data_request import LogDataRequest
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.utils.timezone import utcnow


if typing.TYPE_CHECKING:
    from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

# keep it below VALUE_PREVIEW_MAX_LEN at web
_DEFAULT_VALUE_PREVIEW_MAX_LEN = 10000


@attr.s(slots=True)
class ValueMeta(object):
    value_preview = attr.ib()  # type: str
    data_dimensions = attr.ib(default=None)  # type: Optional[Sequence[int]]
    data_schema = attr.ib(default=None)  # type: Optional[Dict[str,Any]]
    data_hash = attr.ib(default=None)  # type: Optional[str]
    descriptive_stats = attr.ib(
        default=None
    )  # type: Optional[Dict[str, Dict[str, Union[int, float]]]]
    histograms = attr.ib(default=None)  # type: Optional[Dict[str, Tuple]]
    histogram_system_metrics = attr.ib(default=None)  # type: Optional[Dict]

    def build_metrics_for_key(self, key, meta_conf=None):
        # type: (str, Optional[ValueMetaConf]) -> Dict[str, List[Metric]]
        ts = utcnow()
        dataframe_metric_value = {}
        data_metrics, hist_metrics = [], []
        metric_source = MetricSource.user
        if self.data_dimensions:
            dataframe_metric_value["data_dimensions"] = self.data_dimensions
            for dim, size in enumerate(self.data_dimensions):
                key_name = "{}.shape{}".format(key, dim)
                self.append_metric(data_metrics, key_name, metric_source, size, ts)
                name = "rows" if dim == 0 else "columns"
                key_name = "{}.{}".format(key, name)
                self.append_metric(data_metrics, key_name, metric_source, size, ts)

        if meta_conf and meta_conf.log_schema:
            dataframe_metric_value["schema"] = self.data_schema
            key_name = "{}.schema".format(key)
            self.append_metric(
                data_metrics, key_name, metric_source, None, ts, self.data_schema
            )

        if meta_conf and meta_conf.log_preview:
            dataframe_metric_value["value_preview"] = self.value_preview
            dataframe_metric_value["type"] = "dataframe_metric"
            key_name = str(key)
            self.append_metric(
                data_metrics, key_name, metric_source, None, ts, dataframe_metric_value
            )

        metric_source = MetricSource.histograms
        if self.histogram_system_metrics:
            key_name = "{}.histogram_system_metrics".format(key)
            self.append_metric(
                hist_metrics,
                key_name,
                metric_source,
                None,
                ts,
                self.histogram_system_metrics,
            )

        if self.histograms:
            key_name = "{}.histograms".format(key)
            self.append_metric(
                hist_metrics, key_name, metric_source, None, ts, self.histograms
            )

        if self.descriptive_stats:
            key_name = "{}.stats".format(key)
            self.append_metric(
                hist_metrics, key_name, metric_source, None, ts, self.descriptive_stats
            )
            for column, stats in self.descriptive_stats.items():
                for stat, value in stats.items():
                    key_name = "{}.{}.{}".format(key, column, stat)
                    self.append_metric(hist_metrics, key_name, metric_source, value, ts)
        return {"user": data_metrics, "histograms": hist_metrics}

    def append_metric(
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
            log_preview=False, log_histograms=False, log_stats=False, log_size=False,
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
