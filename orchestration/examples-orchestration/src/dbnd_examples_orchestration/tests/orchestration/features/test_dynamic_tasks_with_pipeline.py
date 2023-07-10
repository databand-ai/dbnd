# © Copyright Databand.ai, an IBM Company 2022

from dbnd_examples_orchestration.orchestration.features.dynamic_tasks_with_pipeline import (
    say_hello_to_everybody,
)


class TestDynamicTasks(object):
    def test_run_say_hello_to_everybody(self):
        result = say_hello_to_everybody()
        assert (
            "Hey, user 2!",
            "Hey, some_user! and user 0 and user 1 and user 2",
        ) == result
