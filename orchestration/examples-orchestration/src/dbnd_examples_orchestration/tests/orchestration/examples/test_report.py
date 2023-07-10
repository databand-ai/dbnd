# © Copyright Databand.ai, an IBM Company 2022

from dbnd_examples_orchestration.orchestration.examples.report import (
    dump_db,
    partners_report,
)

from dbnd_run.testing.helpers import assert_run_task


class TestTutorialReportExamples(object):
    def test_tutorial_report_dump(self):
        task = assert_run_task(dump_db.task("aa,bb"))
        assert task

    def test_tutorial_partners_report(self):
        task = assert_run_task(partners_report.task([1, 2]))
        assert task
