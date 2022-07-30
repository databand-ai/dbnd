# © Copyright Databand.ai, an IBM Company 2022

import uuid

from dbnd import config, dbnd_run_cmd, parameter, task


@task(p=parameter[dict])
def task_test():
    print("hello")


class TestInlineCliCalls(object):
    def test_sanity_check(self):
        dbnd_run_cmd(["dbnd_sanity_check"])

    def test_run_with_extend_list(self):
        my_run = dbnd_run_cmd(["dbnd_sanity_check", "--extend", "core.tracker=debug"])
        assert "debug" in my_run.context.settings.core.tracker

    def test_run_with_extend_dict(self):
        with config({"task_test": {"p": {"value": "2"}}}):
            my_run = dbnd_run_cmd(["task_test", "--extend", "task_test.p={'t':'v'}"])
            assert my_run.task.p == {"value": "2", "t": "v"}

    def test_run_with_overridden_run_uid(self):
        expected_run_uid = str(uuid.uuid1())
        my_run = dbnd_run_cmd(
            ["dbnd_sanity_check", "--override-run-uid", expected_run_uid]
        )
        assert str(my_run.run_uid) == expected_run_uid
        assert my_run.existing_run is False
