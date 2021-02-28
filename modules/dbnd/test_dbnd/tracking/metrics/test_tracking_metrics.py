from operator import attrgetter

import pytest

from more_itertools import one

from dbnd import log_metric, log_metrics, task
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_log_metrics


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestTrackingMetrics(object):
    @pytest.mark.parametrize(
        "value, attribute",
        [(1, "value_int"), (0.2, "value_float"), ("asdasd", "value_str"),],
    )
    def test_log_metric(self, mock_channel_tracker, value, attribute):
        @task()
        def task_with_log_metric():
            log_metric(key="test", value=value)

        task_with_log_metric()

        # will raise if no exist
        metric_info = one(get_log_metrics(mock_channel_tracker))
        metric = metric_info["metric"]

        assert metric.value == getattr(metric, attribute)
        assert metric.value == value

    def test_log_metrics(self, mock_channel_tracker):
        @task()
        def task_with_log_metrics():
            # all lower alphabet chars -> {"a": 97,..., "z": 122}
            log_metrics({chr(i): i for i in range(97, 123)})

        task_with_log_metrics()
        metrics_info = list(get_log_metrics(mock_channel_tracker))
        assert len(metrics_info) == 26

        for metric_info in metrics_info:
            metric = metric_info["metric"]
            assert metric.value == metric.value_int
            assert chr(metric.value) == metric.key
