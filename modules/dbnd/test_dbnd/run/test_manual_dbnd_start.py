import logging
import os
import re
import sys

from dbnd import dbnd_run_start, dbnd_run_stop, task
from dbnd.testing.helpers import run_dbnd_subprocess


FAIL_F2 = "fail_f2"
FAIL_MAIN = "fail_main"
USE_DBND_START = "use_dbnd_start"
USE_DBND_STOP = "use_dbnd_stop"

RE_TASK_COMPLETED = r"Task {}[_\w]*.+ has been completed!"
RE_TASK_FAILED = r"Task {}[_\w]*.+ has failed!"
RE_F_RUNNING = r"Running {} function"

CURRENT_PY_FILE = __file__.replace(".pyc", ".py")


def run_dbnd_subprocess__current_file(args, **kwargs):
    args = args or []
    return run_dbnd_subprocess([sys.executable, CURRENT_PY_FILE] + args, **kwargs)


def _assert_output(reg_exp, output, count=1):
    logging.info("RE: %s", reg_exp)
    assert count == len(re.findall(reg_exp, output))


class TestManualDbndStart(object):
    auto_task_name = os.path.basename(__file__)
    expected_task_names = (("f1", 1), ("f2", 3))

    def test_manual_dbnd_start(self):
        result = run_dbnd_subprocess__current_file([USE_DBND_START])
        assert "Running Databand!" in result
        assert "Run tracking info has been committed" in result

        for task_name, count in self.expected_task_names + ((self.auto_task_name, 1),):
            assert count == len(re.findall(RE_TASK_COMPLETED.format(task_name), result))

        for task_name, count in self.expected_task_names:
            assert count == len(re.findall(RE_F_RUNNING.format(task_name), result))

    def test_no_dbnd_start(self):
        result = run_dbnd_subprocess__current_file([])
        assert "Running Databand!" not in result
        assert "Run tracking info has been committed" not in result

        for task_name, count in self.expected_task_names + ((self.auto_task_name, 1),):
            assert 0 == len(re.findall(r"Task {}__\w+".format(task_name), result))

        for task_name, count in self.expected_task_names:
            _assert_output(RE_F_RUNNING.format(task_name), result, count)

    def test_manual_dbnd_start_manual_stop(self):
        run_dbnd_subprocess__current_file([USE_DBND_START, USE_DBND_STOP])

    def test_manual_dbnd_start_fail_main(self):
        result = run_dbnd_subprocess__current_file(
            [USE_DBND_START, FAIL_MAIN], retcode=1
        )
        assert "main bummer!" in result
        _assert_output(RE_TASK_FAILED.format("test_manual_dbnd_start.py"), result)

    def test_manual_dbnd_start_fail_f2(self):
        result = run_dbnd_subprocess__current_file([USE_DBND_START, FAIL_F2], retcode=1)
        assert "f2 bummer!" in result

        for task_name, count in self.expected_task_names:
            _assert_output(RE_TASK_FAILED.format(task_name), result)


@task
def f2(p):
    print("Running f2 function")

    if FAIL_F2 in sys.argv:
        raise Exception("f2 bummer!")

    return p * p


@task
def f1():
    print("Running f1 function")
    sum = 0

    for i in range(1, 4):
        sum += f2(i)

    assert sum == 14


if __name__ == "__main__":
    if USE_DBND_START in sys.argv:
        dbnd_run_start()
        dbnd_run_start()

    f1()

    print("Done")

    if FAIL_MAIN in sys.argv:
        raise Exception("main bummer!")

    if USE_DBND_STOP in sys.argv:
        dbnd_run_stop()
        dbnd_run_stop()
