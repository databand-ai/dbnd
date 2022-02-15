from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.examples.report import dump_db, partners_report


class TestTutorialReportExamples(object):
    def test_tutorial_report_dump(self):
        task = assert_run_task(dump_db.task("aa,bb"))
        assert task

    def test_tutorial_partners_report(self):
        task = assert_run_task(partners_report.task([1, 2]))
        assert task
