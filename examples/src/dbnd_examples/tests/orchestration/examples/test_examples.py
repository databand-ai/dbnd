from typing import List

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.examples.report import dump_db, partners_report
from dbnd_examples.orchestration.examples.top_artists import (
    top_artists_big_report,
    top_artists_report,
)


class TestTutorialExamples(object):
    def test_top_artists_report(self):
        task = assert_run_task(top_artists_report.task())
        actual = task.result.load(List[str])
        assert len(actual) == 10

    def test_top_artists_big_report(self):
        task = assert_run_task(top_artists_big_report.task())
        actual = task.result.load(List[str])
        assert len(actual) == 15

    def test_tutorial_report_dump(self):
        task = assert_run_task(dump_db.task("aa,bb"))
        assert task

    def test_tutorial_partners_report(self):
        task = assert_run_task(partners_report.task([1, 2]))
        assert task
