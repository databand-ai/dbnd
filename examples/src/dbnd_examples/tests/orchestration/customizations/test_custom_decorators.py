from dbnd_examples.orchestration.customizations.custom_decorator import (
    my_new_experiement,
)


class TestCustomDecorator(object):
    def test_my_new_experiement(self):
        task = my_new_experiement.dbnd_run().root_task
        assert task
