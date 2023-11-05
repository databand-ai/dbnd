# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.task_build.task_passport import TaskPassport


class TestTaskPassport(object):
    def test_from_module(self):
        actual = TaskPassport.from_module(self.__class__.__module__)
        assert actual

    def test_build_task_passport_simple(self):
        actual = TaskPassport.build_task_passport(
            "cname", "mname", "nname", task_family="fname"
        )
        assert actual.full_task_family == "mname.cname"
        assert actual.full_task_family_short == "m.cname"

    def test_build_task_passport_empty_module(self):
        actual = TaskPassport.build_task_passport(
            "cname", None, "nname", task_family="fname"
        )
        assert actual.full_task_family == "__main__.cname"
        assert actual.full_task_family_short == "_.cname"
