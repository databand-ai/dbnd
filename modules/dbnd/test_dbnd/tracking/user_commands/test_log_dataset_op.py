import itertools
import json
import os

import pandas as pd
import pytest

from dbnd import task
from dbnd._core.constants import DbndDatasetOperationType
from dbnd._core.tracking.metrics import log_dataset_op
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_log_metrics, get_log_targets


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestLogDataSetOpMetrics(object):
    @pytest.mark.parametrize(
        "preview,schema",
        # repeat the tests for each combination of the flags -> 2^2 tests == 4 tests!!
        list(itertools.product([False, True], repeat=2)),
    )
    def test_log_dataset_op_nested_json_data(
        self, mock_channel_tracker, preview, schema,
    ):
        with open(THIS_DIR + "/nested_data.json", encoding="utf-8-sig") as f:
            nested_json = pd.json_normalize(json.load(f))

        @task()
        def task_log_dataset_op_nested_json_data():
            log_dataset_op(
                op_path="/my/path/to/nested_data.json",
                op_type=DbndDatasetOperationType.read,
                data=nested_json,
                with_schema=schema,
                with_preview=preview,
                with_histograms=True,
                with_partition=False,
            )

        task_log_dataset_op_nested_json_data()
        metrics_info = list(get_log_metrics(mock_channel_tracker))
        map_metrics = {
            metric_info["metric"].key: metric_info["metric"]
            for metric_info in metrics_info
        }

        print(
            "preview={preview}, schema={schema}".format(preview=preview, schema=schema,)
        )

        for m in metrics_info:
            print(m["metric"], m["metric"].value)

        assert "my.path.to.nested_data.json.shape0" in map_metrics
        assert map_metrics["my.path.to.nested_data.json.shape0"].value == 3
        assert "my.path.to.nested_data.json.shape1" in map_metrics
        assert map_metrics["my.path.to.nested_data.json.shape1"].value == 22

        # Tests for schema
        # ------------------
        # Only report schema if the schema flag is on, the schema source is the user.
        assert if_and_only_if(
            schema,
            (
                "my.path.to.nested_data.json.schema" in map_metrics
                and map_metrics["my.path.to.nested_data.json.schema"].source == "user"
            ),
        )

        #
        # Size flag is used only with schema flag
        # together they add a size.bytes calculation in the schema value
        assert if_and_only_if(
            schema,
            (
                "my.path.to.nested_data.json.schema" in map_metrics
                and "size.bytes"
                in map_metrics["my.path.to.nested_data.json.schema"].value
            ),
        )

        # Tests for preview
        # ------------------
        # When preview is on we expect to have a the value sent with a preview
        assert if_and_only_if(
            preview,
            (
                "my.path.to.nested_data.json" in map_metrics
                and "value_preview" in map_metrics["my.path.to.nested_data.json"].value
            ),
        )

        #
        # When we have both preview and schema we expect to have a schema part of the value metric
        assert if_and_only_if(
            (preview and schema),
            (
                "my.path.to.nested_data.json" in map_metrics
                and "schema" in map_metrics["my.path.to.nested_data.json"].value
            ),
        )
        #
        # When we preview, schema and size we expect the the preview inside the schema inside the value
        # would have size.bytes value
        assert if_and_only_if(
            (preview and schema),
            (
                "my.path.to.nested_data.json" in map_metrics
                and "schema" in map_metrics["my.path.to.nested_data.json"].value
                and "size.bytes"
                in map_metrics["my.path.to.nested_data.json"].value["schema"]
            ),
        )

        #
        # Only report columns if the schema flag is on, the schema columns is expected.
        expected_columns = {
            "greeting",
            "longitude",
            "eyeColor",
            "address",
            "name",
            "age",
            "isActive",
            "tags",
            "guid",
            "about",
            "index",
            "balance",
            "email",
            "phone",
            "registered",
            "latitude",
            "_id",
            "favoriteFruit",
            "picture",
            "gender",
            "friends",
            "company",
        }
        if schema:
            assert set(
                map_metrics["my.path.to.nested_data.json.schema"].value["columns"]
            ) == set(expected_columns)

    @pytest.mark.parametrize(
        "preview,schema",
        # repeat the tests for each combination of the flags -> 2^2 tests == 4 tests!!
        list(itertools.product([False, True], repeat=2)),
    )
    def test_log_dataset_op_flat_json_data(
        self, mock_channel_tracker, preview, schema,
    ):
        with open(THIS_DIR + "/flat_data.json", encoding="utf-8-sig") as f:
            flat_json = json.load(f)

        @task()
        def task_log_dataset_op_flat_json_data():
            log_dataset_op(
                op_path="/my/path/to/flat_data.json",
                op_type=DbndDatasetOperationType.read,
                data=flat_json,
                with_schema=schema,
                with_preview=preview,
                with_histograms=False,
                with_partition=False,
            )

        task_log_dataset_op_flat_json_data()
        metrics_info = list(get_log_metrics(mock_channel_tracker))
        map_metrics = {
            metric_info["metric"].key: metric_info["metric"]
            for metric_info in metrics_info
        }

        print(
            "preview={preview}, schema={schema}".format(preview=preview, schema=schema,)
        )

        for m in metrics_info:
            print(m["metric"], m["metric"].value)

        assert "my.path.to.flat_data.json.shape0" in map_metrics
        assert map_metrics["my.path.to.flat_data.json.shape0"].value == 6
        assert "my.path.to.flat_data.json.shape1" in map_metrics
        assert map_metrics["my.path.to.flat_data.json.shape1"].value == 5

        # Tests for schema
        # ------------------
        # Only report schema if the schema flag is on, the schema source is the user.
        assert if_and_only_if(
            schema,
            (
                "my.path.to.flat_data.json.schema" in map_metrics
                and map_metrics["my.path.to.flat_data.json.schema"].source == "user"
            ),
        )
        #
        # Size flag is used only with schema flag
        # together they add a size.bytes calculation in the schema value
        assert if_and_only_if(
            schema,
            (
                "my.path.to.flat_data.json.schema" in map_metrics
                and "size.bytes"
                in map_metrics["my.path.to.flat_data.json.schema"].value
            ),
        )

        # Tests for preview
        # ------------------
        # When preview is on we expect to have a the value sent with a preview
        assert if_and_only_if(
            preview,
            (
                "my.path.to.flat_data.json" in map_metrics
                and "value_preview" in map_metrics["my.path.to.flat_data.json"].value
            ),
        )
        #
        # When we have both preview and schema we expect to have a schema part of the value metric
        assert if_and_only_if(
            (preview and schema),
            (
                "my.path.to.flat_data.json" in map_metrics
                and "schema" in map_metrics["my.path.to.flat_data.json"].value
            ),
        )
        #
        # When we preview, schema and size we expect the the preview inside the schema inside the value
        # would have size.bytes value
        assert if_and_only_if(
            (preview and schema),
            (
                "my.path.to.flat_data.json" in map_metrics
                and "schema" in map_metrics["my.path.to.flat_data.json"].value
                and "size.bytes"
                in map_metrics["my.path.to.flat_data.json"].value["schema"]
            ),
        )

        # Only report columns if the schema flag is on, the schema columns is expected.
        expected_columns = {
            "Leave",
            "Serial Number",
            "Employee Markme",
            "Description",
            "Company Name",
        }
        if schema:
            assert (
                set(map_metrics["my.path.to.flat_data.json.schema"].value["columns"])
                == expected_columns
            )


def if_and_only_if(left, right):
    return not (left ^ right)
