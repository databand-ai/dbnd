# © Copyright Databand.ai, an IBM Company 2022

from typing import Set

from dbnd import parameter
from dbnd.testing.helpers import build_task
from dbnd.testing.orchestration_utils import TTask


class SetParameterTask(TTask):
    param = parameter[Set]
    param_typed = parameter[Set].sub_type(int)


class TestSetParameter(object):
    def test_parse_simple(self):
        assert {"1", "2", "3"} == parameter[Set]._p.parse_from_str("1,2,3")

    def test_serialize_task(self):
        p = parameter[Set]._p
        assert p.to_str({1, 2, 3}) == p.to_str({2, 1, 3})

    def test_parse_object(self):
        task = SetParameterTask(param="[1,2]", param_typed="['2','3']")
        assert task.param == {1, 2}
        assert task.param_typed == {2, 3}

    def test_same_sig(self):
        task1 = SetParameterTask(param={"a", "b"}, param_typed={1, 2, 3})
        assert task1.param == {"a", "b"}
        task2 = SetParameterTask(param={"a", "b"}, param_typed={1, 2, 3})
        assert task1.task_id == task2.task_id
        assert id(task1) == id(task2)

    def test_parse_cmd_line(self):
        task = build_task("SetParameterTask", param="[1,2]", param_typed="['2','3']")
        assert task.param == {1, 2}
        assert task.param_typed == {2, 3}
