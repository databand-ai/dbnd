from __future__ import absolute_import

import logging
import shutil

from random import randint, random

import mock
import pytest

from dbnd import new_dbnd_context
from dbnd._core.decorator.func_task_decorator import task
from mlflow import (
    end_run,
    get_tracking_uri,
    log_artifacts,
    log_metric,
    log_param,
    start_run,
)


logger = logging.getLogger(__name__)


@task
def mlflow_tracking_integration_check():
    start_run()
    # params
    log_param("param1", randint(0, 100))
    log_param("param2", randint(0, 100))
    # metrics
    log_metric("foo1", random())
    log_metric("foo1", random() + 1)
    log_metric("foo2", random())
    log_metric("foo2", random() + 1)
    end_run()


class TestMLFlowTrackingIntegration(object):
    def test_plugin_loading(self):
        with new_dbnd_context(
            conf={
                "core": {"databand_url": "https://secure"},
                "mlflow_tracking": {
                    "databand_tracking": True,
                    "duplicate_tracking_to": "http://mlflow",
                },
            }
        ):
            assert (
                get_tracking_uri()
                == "dbnd+s://secure?duplicate_tracking_to=http%253A%252F%252Fmlflow"
            )

    def test_dbnd_store_initialization(self):
        with new_dbnd_context(
            conf={"mlflow_tracking": {"databand_tracking": True}}
        ), mock.patch(
            "dbnd_mlflow.tracking_store.TrackingApiClient"
        ) as fake_tracking_store:
            mlflow_tracking_integration_check.dbnd_run()
            fake_tracking_store.assert_called_once_with("http://localhost:8080")

        with new_dbnd_context(
            conf={
                "core": {"databand_url": "https://secure"},
                "mlflow_tracking": {"databand_tracking": True},
            }
        ), mock.patch(
            "dbnd_mlflow.tracking_store.TrackingApiClient"
        ) as fake_tracking_store:
            mlflow_tracking_integration_check.dbnd_run(task_version="now")
            fake_tracking_store.assert_called_once_with("https://secure")

    def test_run(self):
        with new_dbnd_context(
            conf={"mlflow_tracking": {"databand_tracking": True}}
        ), mock.patch(
            "dbnd_mlflow.tracking_store.TrackingStoreApi.log_metric"
        ) as fake_log_metric:
            mlflow_tracking_integration_check.dbnd_run(task_version="now")
            assert fake_log_metric.call_count == 6

    @classmethod
    def teardown_class(cls):
        shutil.rmtree("mlruns/")
