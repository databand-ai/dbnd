import pdb
import random

import mock
import pytest

from databand import dbnd_config
from dbnd._core.errors import DatabandRunError
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline
from targets import target


TEXT_FILE = "/app/integration-test/vegetables_for_greek_salad.txt"


def extract_driver_log_uri(uri, proxy="https://proxy.proxy.local:12345/gateway/"):
    from urllib.parse import urlparse

    result = urlparse(uri)
    host, port = result.netloc.split(":")
    return f"{proxy}/resource?scheme={result.scheme}&host={host}&port={port}"


def add_proxy_as_external_link(spark_ctrl, batch_response):
    from dbnd._core.tracking.commands import set_external_resource_urls

    url = spark_ctrl.get_livy_endpoint().url
    proxy_url = extract_driver_log_uri(url)
    set_external_resource_urls({"proxy": proxy_url})


STATE = False


def side_effect(*args, **kwargs):
    global STATE
    STATE = True


class SpecialException(Exception):
    pass


def hook_with_raise(*args, **kwargs):
    raise SpecialException()


conf_override = {
    "task": {"spark_engine": "livy"},
    "livy": {"url": "http://livy:8998",},
    SparkConfig.jars: "",
    SparkConfig.main_jar: "/app/spark_jvm/target/ai.databand.examples-1.0-SNAPSHOT.jar",
    "log": {"level": "DEBUG"},
}


@pytest.fixture
def mock_channel_tracker():
    with mock.patch(
        "dbnd._core.tracking.backends.tracking_store_channels.TrackingStoreThroughChannel._m"
    ) as mock_store:
        yield mock_store


class TestEmrSparkTasks(object):
    def test_word_count_spark(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            actual = WordCountTask(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
            actual.dbnd_run()
            print(target(actual.counters.path, "part-00000").read())

    def test_word_count_pyspark(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            actual = WordCountPySparkTask(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
            actual.dbnd_run()
            print(target(actual.counters.path, "part-00000").read())

    @pytest.mark.skip("Some issue with cloud pickler")
    def test_word_spark_with_error(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            actual = WordCountThatFails(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
            with pytest.raises(DatabandRunError):
                actual.dbnd_run()

    def test_word_count_inline(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            assert_run_task(
                word_count_inline.t(
                    text=TEXT_FILE,
                    task_version=str(random.random()),
                    override=conf_override,
                )
            )

    def test_word_count_with_hook(self, mock_channel_tracker):
        _config = conf_override.copy()

        # add post submit hook location to the livy config
        _config["livy"][
            "post_submit_hook"
        ] = "test_livy_spark_task.add_proxy_as_external_link"

        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            WordCountTask(
                text=TEXT_FILE, task_version=str(random.random()), override=_config,
            ).dbnd_run()

        calls = [
            call
            for call in mock_channel_tracker.call_args_list
            if call.args[0].__name__ == "save_external_links"
        ]
        assert len(calls) == 2
        assert calls[1].kwargs["external_links_dict"] == {
            "proxy": "https://proxy.proxy.local:12345/gateway//resource?scheme=http&host=livy&port=8998"
        }

    def test_word_count_with_hook_side_effect(self):
        _config = conf_override.copy()

        global STATE
        STATE = False

        # add post submit hook location to the livy config
        _config["livy"]["post_submit_hook"] = "test_livy_spark_task.side_effect"

        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            WordCountTask(
                text=TEXT_FILE, task_version=str(random.random()), override=_config,
            ).dbnd_run()

        # calling the hook will call side_effect which will change the STATE to True
        assert STATE

    def test_word_count_with_hook_raise(self):
        _config = conf_override.copy()

        # add post submit hook location to the livy config
        _config["livy"]["post_submit_hook"] = "test_livy_spark_task.hook_with_raise"
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            with pytest.raises(DatabandRunError):
                WordCountTask(
                    text=TEXT_FILE, task_version=str(random.random()), override=_config,
                ).dbnd_run()
