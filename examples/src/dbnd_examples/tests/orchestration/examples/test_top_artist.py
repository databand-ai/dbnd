from typing import List

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.examples.top_artists import (
    top_artists_big_report,
    top_artists_report,
)


class TestTopArtistExamples(object):
    def test_top_artists_report(self):
        task = assert_run_task(top_artists_report.task())
        actual = task.result.load(List[str])
        assert len(actual) == 10

    def test_top_artists_big_report(self):
        task = assert_run_task(top_artists_big_report.task())
        actual = task.result.load(List[str])
        assert len(actual) == 15
