import typing

import attr

from dbnd._core.constants import MetricSource
from dbnd._core.tracking.log_data_reqeust import LogDataRequest
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

        if self.data_dimensions:
            dataframe_metric_value["data_dimensions"] = self.data_dimensions
            for dim, size in enumerate(self.data_dimensions):
                data_metrics.append(
                    Metric(
                        key="{}.shape{}".format(key, dim),
                        value=size,
                        source=MetricSource.user,
                        timestamp=ts,
                    )
                )

        if meta_conf and meta_conf.log_schema:
            dataframe_metric_value["schema"] = self.data_schema
            data_metrics.append(
                Metric(
                    key="{}.schema".format(key),
                    value_json=self.data_schema,
                    source=MetricSource.user,
                    timestamp=ts,
                )
            )

        if meta_conf and meta_conf.log_preview:
            dataframe_metric_value["value_preview"] = self.value_preview
            dataframe_metric_value["type"] = "dataframe_metric"
            data_metrics.append(
                Metric(
                    key=str(key),
                    value_json=dataframe_metric_value,
                    source=MetricSource.user,
                    timestamp=ts,
                )
            )

        if self.histogram_system_metrics:
            hist_metrics.append(
                Metric(
                    key="{}.histogram_system_metrics".format(key),
                    value_json=self.histogram_system_metrics,
                    source=MetricSource.histograms,
                    timestamp=ts,
                )
            )
        if self.histograms:
            hist_metrics.append(
                Metric(
                    key="{}.histograms".format(key),
                    value_json=self.histograms,
                    source=MetricSource.histograms,
                    timestamp=ts,
                )
            )
        if self.descriptive_stats:
            hist_metrics.append(
                Metric(
                    key="{}.stats".format(key),
                    value_json=self.descriptive_stats,
                    source=MetricSource.histograms,
                    timestamp=ts,
                )
            )
            for column, stats in self.descriptive_stats.items():
                for stat, value in stats.items():
                    hist_metrics.append(
                        Metric(
                            key="{}.{}.{}".format(key, column, stat),
                            value=value,
                            source=MetricSource.histograms,
                            timestamp=ts,
                        )
                    )
        return {"user": data_metrics, "histograms": hist_metrics}


@attr.s
class ValueMetaConf(object):
    log_preview = attr.ib(default=None)  # type: Optional[bool]
    log_preview_size = attr.ib(default=None)  # type: Optional[int]
    log_schema = attr.ib(default=None)  # type: Optional[bool]
    log_size = attr.ib(default=None)  # type: Optional[bool]
    log_histograms = attr.ib(
        default=LogDataRequest.NONE(), converter=LogDataRequest.from_user_param
    )  # type: Optional[Union[LogDataRequest, bool]]
    log_stats = attr.ib(
        default=LogDataRequest.NONE(), converter=LogDataRequest.from_user_param
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
