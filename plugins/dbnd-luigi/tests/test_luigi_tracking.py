import datetime
import logging
import random
import sys

import mock
import pytest

from dbnd import dbnd_config
from dbnd._core.settings import CoreConfig
from dbnd_luigi.luigi_tracking import dbnd_luigi_build, dbnd_luigi_run
from luigi import LuigiStatusCode
from luigi.date_interval import Custom
from tests.luigi_examples.top_artists import (
    Streams,
    Top10Artists,
    Top10ArtistsOutputException,
    Top10ArtistsRequiresException,
    Top10ArtistsRunException,
)


# logger = logging.getLogger(__name__)


class TestLuigiExecution(object):
    @pytest.fixture(autouse=True)
    def clean_output(self):
        import os

        os.system("rm -rf ./data/*")

    @pytest.fixture(autouse=True)
    def date_a(self):
        return datetime.date(year=2019, month=1, day=1)

    @pytest.fixture(autouse=True)
    def date_b(self, date_a):
        return datetime.date(year=2019, month=1, day=3)

    @pytest.fixture(autouse=True)
    def date_interval(self, date_a, date_b):
        return Custom(date_a, date_b)

    @pytest.fixture(autouse=True)
    def streams(self, date_a):
        return Streams(date=date_a)

    @pytest.fixture(autouse=True)
    def top10_artists(self, date_interval):
        return Top10Artists(date_interval=date_interval)

    @pytest.fixture(autouse=True)
    def top10_artists_run_error(self, date_interval):
        return Top10ArtistsRunException(date_interval=date_interval)

    @pytest.fixture(autouse=True)
    def top10_artists_requires_error(self, date_interval):
        return Top10ArtistsRequiresException(date_interval=date_interval)

    @pytest.fixture(autouse=True)
    def top10_artists_output_error(self, date_interval):
        return Top10ArtistsOutputException(date_interval=date_interval)

    def test_luigi_sanity(self, top10_artists):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            # result = dbnd_luigi_build(
            #     tasks=[top10_artists],
            # )
            result = dbnd_luigi_build(tasks=[top10_artists])
        assert result.status == LuigiStatusCode.SUCCESS

    def test_luigi_orphan_task(self, streams):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            result = dbnd_luigi_build(tasks=[streams])
            assert result.status == LuigiStatusCode.SUCCESS

    def test_luigi_run_exception(self):
        sys.argv = [
            "luigi",
            "Top10ArtistsRunException",
            "--Top10ArtistsRunException-date-interval",
            "2020-05-02",
            "--local-scheduler",
            "--module",
            str("tests.luigi_examples.top_artists"),
        ]
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with mock.patch("dbnd_luigi.luigi_tracking.lrm") as lrm_patch:
                lrm_patch.events_active = False
                result = dbnd_luigi_run()
                assert lrm_patch.on_failure.call_count == 1
                assert lrm_patch.on_success.call_count == 2
                assert lrm_patch.on_dependency_discovered.call_count == 2
                assert lrm_patch.on_run_start.call_count == 3
                assert result.status == LuigiStatusCode.FAILED

    def test_luigi_build_exception(self, top10_artists_run_error):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with mock.patch("dbnd_luigi.luigi_tracking.lrm") as lrm_patch:
                lrm_patch.events_active = False
                result = dbnd_luigi_build(tasks=[top10_artists_run_error])
                assert lrm_patch.on_failure.call_count == 1
                assert lrm_patch.on_success.call_count == 3
                assert lrm_patch.on_dependency_discovered.call_count == 3
                assert lrm_patch.on_run_start.call_count == 4
                assert result.status == LuigiStatusCode.FAILED

    def test_luigi_requires_exception(self, top10_artists_requires_error):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            result = dbnd_luigi_build(tasks=[top10_artists_requires_error])
            assert result.status == LuigiStatusCode.SCHEDULING_FAILED

    def test_luigi_output_exception(self, top10_artists_output_error):
        with pytest.raises(Exception):
            with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
                result = dbnd_luigi_build(tasks=[top10_artists_output_error])
                # Exception in output leads to scheduling failed because Luigi starts by checking if output exists
                assert result.status == LuigiStatusCode.SCHEDULING_FAILED

    # TODO:
    # def test_luigi_output_target_tracking(self):
    # def test_luigi_input_target_tracking(self):
