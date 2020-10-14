import logging
import os
import subprocess

from timeit import default_timer as timer

from dbnd import task
from dbnd._core.cli.service_auto_completer import completer
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd.testing.helpers import run_dbnd_subprocess__dbnd


def _auto_complete(use_fast_subprocess=False):
    kwargs = dict()

    if not use_fast_subprocess:
        kwargs.update(dict(stdout=subprocess.PIPE))

    return run_dbnd_subprocess__dbnd(
        args=[],
        blocking=use_fast_subprocess,
        env={
            "COMP_WORDS": "dbnd ",
            "COMP_CWORD": "1",
            "_DBND_COMPLETE": "complete_zsh",
            "LANG": os.environ.get("LANG", "en_US.UTF8"),
        },
        **kwargs
    )


class TestAutoComplete(object):
    def test_response_time(self):
        start = timer()
        _auto_complete().wait()
        end = timer()
        elapsed = end - start

        assert elapsed < 10  # CI/CD job can be really slow

    def test_bad_lines(self):
        p = _auto_complete()

        lines = []
        for line in iter(p.stdout.readline, b""):
            lines.append(line.decode())

        # find and print bad lines
        def _is_bad(line):
            for word in ["DeprecationWarning", "site-packages", "/Users/"]:
                if word in line:
                    return True
            return False

        msg = "There are bad lines in auto completion output!\n\n"
        has_bad_line = False
        for line in lines:
            if _is_bad(line):
                has_bad_line = True
                msg += "BAD LINE: " + line
            else:
                msg += "  " + line

        assert has_bad_line is False, msg

    def test_auto_complete_renew(self):
        @task
        def my_task_autocomplete(a):
            # type: (int)->str
            """
            my task help
            """
            return "ok"

        task_registry = get_task_registry()
        task_classes = task_registry.list_dbnd_task_classes()
        logging.info("task_classes: %s", list(task_registry.list_dbnd_task_classes()))
        completer.refresh(task_classes)
        task_completer = completer.task()
        actual = task_completer(None, None, "my_tas")
        assert actual == [("my_task_autocomplete", "my task help")]
