# © Copyright Databand.ai, an IBM Company 2022

import os
import tempfile

from typing import Tuple

from dbnd import pipeline, task
from dbnd._core.task_executor.results_view import RunResultBand
from dbnd._core.utils.basics.path_utils import relative_path
from dbnd.testing.helpers_pytest import assert_run, skip_on_windows
from targets import target
from tests.orchestration.utils import DbndCmdTest


@task
def worker(i):
    # type: (int)->int
    return i * 2


@pipeline(result=("one", "two"))
def driver():
    # type: () -> Tuple[int, int]
    r = [worker(i) for i in range(10)]
    return r[1], r[2]


def test_result_view():
    a = driver.task()
    run = assert_run(a)
    assert run.run_executor.result is not None
    assert run.run_executor.result.load("one") == 2
    assert run.run_executor.result.load("two") == 4


class TestSubmitterHasAccessToResult(DbndCmdTest):
    test_config = os.path.join(relative_path(__file__, "../.."), "databand-test.cfg")

    @skip_on_windows
    def test_submit_access_to_result(self):
        run = self.dbnd_run_task_with_output(
            [
                "--env",
                "local_resubmit",
                "--interactive",
                "--conf-file",
                self.test_config,
            ]
        )

        assert run.run_executor.result.load("t_output") == "1"
        assert run.load_from_result("t_output") == run.run_executor.result.load(
            "t_output"
        )

    @skip_on_windows
    def test_result_view_with_config(self):
        new = target(tempfile.mktemp(), "name.json")
        run = self.dbnd_run_task_with_output(
            [
                "--set",
                "run.run_result_json_path={}".format(new.path),
                "--set",
                "run.execution_date=2020-01-01T101010.00",
                "--run-driver",
                "f8073514-5bff-11eb-ae28-acde48001182",
            ]
        )

        assert run.run_executor.result.load("t_output") == "1"
        result = RunResultBand.from_target(new)
        assert result.load("t_output") == "1"
        assert run.load_from_result("t_output") == result.load("t_output")
