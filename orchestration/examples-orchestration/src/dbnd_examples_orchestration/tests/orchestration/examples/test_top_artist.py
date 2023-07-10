# © Copyright Databand.ai, an IBM Company 2022

from typing import List

from dbnd_examples_orchestration.orchestration.examples.top_artists import (
    top_artists_big_report,
    top_artists_report,
)

from dbnd_run.testing.helpers import assert_run_task


class TestTopArtistExamples(object):
    def test_top_artists_report(self):
        task = assert_run_task(top_artists_report.task())
        actual = task.result.load(List[str])
        assert len(actual) == 10

    def test_top_artists_big_report(self):
        task = assert_run_task(top_artists_big_report.task())
        actual = task.result.load(List[str])
        assert len(actual) == 15
