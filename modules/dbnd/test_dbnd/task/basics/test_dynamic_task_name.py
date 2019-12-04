import uuid

from dbnd._core.inline import run_cmd_locally


class TestDynamicTaskName(object):
    def test_function_cmd_dynamic_name(self):
        dbnd_test_name = "unique_test_name_" + uuid.uuid4().hex.upper()[0:6]
        cmdline = ["dbnd_sanity_check", "--task-name", dbnd_test_name]
        result = run_cmd_locally(cmdline)
        assert result.task.task_name == dbnd_test_name

    def test_class_cmd_dynamic_name(self):
        dbnd_test_name = "unique_test_name_" + uuid.uuid4().hex.upper()[0:6]
        cmdline = ["SimplestTask", "--task-name", dbnd_test_name]
        result = run_cmd_locally(cmdline)
        assert result.task.task_name == dbnd_test_name
