import itertools

import pytest

from more_itertools import first_true, one

from dbnd import log_metric, log_metrics, task
from dbnd._core.tracking.metrics import log_data
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_log_metrics, get_log_targets


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestTrackingMetrics(object):
    @pytest.mark.parametrize(
        "value, attribute",
        [(1, "value_int"), (0.2, "value_float"), ("asdasd", "value_str")],
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

    @pytest.mark.parametrize(
        "preview,size,schema,stats,histograms,path",
        # repeat the tests for each combination of the flags -> 2^6 tests == 64 tests!! this is a terrible interface
        list(itertools.product([False, True], repeat=6)),
    )
    def test_log_data(
        self,
        mock_channel_tracker,
        pandas_data_frame,
        preview,
        size,
        schema,
        stats,
        histograms,
        path,
    ):
        """
        This test is a bit complicated but it cover any flag that there is in this function (almost)
        For each scenario we want to be sure that the expected output sent to the tracker.
        This mean that when a specific flag is set to false we expect that the relevant metrics will *not* be send.

        This is not a test to make sure that the histogram calculations or preview or schema output are as expected.
        This is only a test for the api of log_data/log_dataframe.

        !! This test help us see that this interface is not very intuitive !!
        """

        @task()
        def task_with_log_data():
            log_data(
                key="df",
                value=pandas_data_frame,
                with_preview=preview,
                with_size=size,
                with_schema=schema,
                with_stats=stats,
                with_histograms=histograms,
                path="/my/path/to_file.txt" if path else None,
            )

        task_with_log_data()
        metrics_info = list(get_log_metrics(mock_channel_tracker))
        map_metrics = {
            metric_info["metric"].key: metric_info["metric"]
            for metric_info in metrics_info
        }

        # note: This is a test helper to use when debugging
        # side-note: I wish we didn't had to support py2 and could use f-string
        # side-note: I wish to use the f-string debug available from py3.8
        # https://tirkarthi.github.io/programming/2019/05/08/f-string-debugging.html
        # >>> print(f"{preview=}, {size=}, {schema=}, {stats=},{histograms=}")
        print(
            "preview={preview}, size={size}, schema={schema}, stats={stats}, histograms={histograms}, path={path}".format(
                preview=preview,
                size=size,
                schema=schema,
                stats=stats,
                histograms=histograms,
                path=path,
            )
        )

        for m in metrics_info:
            print(m["metric"], m["metric"].value)

        # no matter which configuration is set we expect to log the shape:
        assert "df.shape0" in map_metrics
        assert map_metrics["df.shape0"].value == 5
        assert "df.shape1" in map_metrics
        assert map_metrics["df.shape1"].value == 3

        # Tests for schema
        # ------------------
        # Only report schema if the schema flag is on, the schema source is the user.
        assert if_and_only_if(
            schema,
            ("df.schema" in map_metrics and map_metrics["df.schema"].source == "user"),
        )
        #
        # Size flag is used only with schema flag
        # together they add a size.bytes calculation in the schema value
        assert if_and_only_if(
            (schema and size),
            (
                "df.schema" in map_metrics
                and "size.bytes" in map_metrics["df.schema"].value
            ),
        )

        # Tests for preview
        # ------------------
        # When preview is on we expect to have a the value sent with a preview
        assert if_and_only_if(
            preview,
            ("df" in map_metrics and "value_preview" in map_metrics["df"].value),
        )
        #
        # When we have both preview and schema we expect to have a schema part of the value metric
        assert if_and_only_if(
            (preview and schema),
            ("df" in map_metrics and "schema" in map_metrics["df"].value),
        )
        #
        # When we preview, schema and size we expect the the preview inside the schema inside the value
        # would have size.bytes value
        assert if_and_only_if(
            (preview and schema and size),
            (
                "df" in map_metrics
                and "schema" in map_metrics["df"].value
                and "size.bytes" in map_metrics["df"].value["schema"]
            ),
        )

        # Tests for histograms
        # ---------------------
        # We only log the histogram metrics when we use the histogram flag
        assert if_and_only_if(
            histograms,
            (
                "df.histograms" in map_metrics
                and map_metrics["df.histograms"].source == "histograms"
                and "df.histogram_system_metrics" in map_metrics
                and map_metrics["df.histogram_system_metrics"].source == "histograms"
            ),
        )
        #
        # This is a tricky one - when we have stats on
        # we create for each of the columns multiple histograms' metrics.
        assert if_and_only_if(
            stats,
            all(
                any(header in metric_name for metric_name in map_metrics)
                for header in pandas_data_frame.columns
            ),
        )

        if path:
            log_target = first_true(
                get_log_targets(mock_channel_tracker),
                pred=lambda t: not t.target_path.startswith("memory://"),
            )
            assert log_target.target_path == "/my/path/to_file.txt"
            # the data dimensions is taken from the data frame
            assert log_target.data_dimensions == (5, 3)

            has_data_schema = bool(eval(log_target.data_schema))
            assert if_and_only_if(schema or size, has_data_schema)


def if_and_only_if(left, right):
    return not (left ^ right)
