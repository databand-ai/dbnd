import logging

from dbnd import task
from dbnd._core.cli.service_auto_completer import completer
from dbnd._core.task_build.task_registry import get_task_registry


class TestAutoComplete(object):

    # def test_autocompletion(self):
    #     current = os.environ
    #     try:
    #         os.environ = {
    #             "_DBND_COMPLETE": "complete_zsh",
    #             "COMP_CWORD": "2",
    #             "COMP_WORDS": "dbnd run ",
    #         }
    #         from dbnd import dbnd_main
    #         dbnd_main()
    #     except:
    #         os.environ = current

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
