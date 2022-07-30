# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd import Config, config, parameter, task
from dbnd._core.context.databand_context import new_dbnd_context


logger = logging.getLogger(__name__)


def _user_code_raises():
    raise Exception("USER_CODE_ERROR")


_user_code_run = False


def _user_code():
    global _user_code_run
    _user_code_run = True


@task
def user_func(a, b):
    assert a == "1"
    assert b == "2"


class MyConfig22(Config):
    config_id = parameter[str]


def inject_some_params():
    my_c = MyConfig22()
    logging.info("my_c: %s", my_c.config_id)
    config.set_values(
        config_values={"user_func": {"a": my_c.config_id, "b": "2"}},
        source="inject_some_params",
    )


class TestDatabandContext(object):
    def test_user_code_run(self):
        with new_dbnd_context(
            conf={
                "core": {
                    "user_init": "test_dbnd.configuration.test_task_context._user_code"
                }
            }
        ):
            pass
        assert _user_code_run, "user code wasn't executed"
        logger.info("done")

    def test_user_config_inject(self):
        with new_dbnd_context(
            conf={
                "core": {
                    "user_init": "test_dbnd.configuration.test_task_context.inject_some_params"
                },
                "MyConfig22": {"config_id": "1"},
            }
        ) as c:
            c.dbnd_run_task(task_or_task_name="user_func")

        logger.info("done")

    def test_user_code_fail(self):
        with pytest.raises(Exception, match=r"USER_CODE_ERROR"):
            with new_dbnd_context(
                conf={
                    "core": {
                        "user_init": "test_dbnd.configuration.test_task_context._user_code_raises"
                    }
                }
            ):
                pass
