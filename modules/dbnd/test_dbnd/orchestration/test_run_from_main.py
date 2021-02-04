import logging
import sys

from dbnd.tasks.basics import SimplestTask
from dbnd.testing.helpers import run_dbnd_subprocess


logger = logging.getLogger(__name__)

CURRENT_PY_FILE = __file__.replace(".pyc", ".py")


class TestTaskInplaceRun(object):
    def test_inplace_run(self):
        run_dbnd_subprocess([sys.executable, CURRENT_PY_FILE, "new_dbnd_context"])


if __name__ == "__main__":
    SimplestTask(task_env="local").dbnd_run()
