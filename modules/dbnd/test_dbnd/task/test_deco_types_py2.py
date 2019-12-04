import logging

from typing import Optional, Tuple, Union

from dbnd import task
from test_dbnd.targets_tests import TargetTestBase


logger = logging.getLogger(__name__)


class TestDecoTypesPy2(TargetTestBase):
    def test_simple_deco(self):
        @task
        def t_f_defaults(a=5):
            # type: (int)->str
            # some comment
            assert a == 5
            return "ok"

        assert t_f_defaults.task.a.value_type.type == int
        assert t_f_defaults.task.result.value_type.type == str

    def test_optional_deco(self):
        @task
        def t_f_defaults(a=5, b=2):
            # type: (Optional[int], Optional[str])->Optional[str]
            # some comment
            assert a == 5
            return "ok"

        assert t_f_defaults.task.a.value_type.type == int
        assert t_f_defaults.task.b.value_type.type == str
        assert t_f_defaults.task.result.value_type.type == str

    def test_optional_tuple(self):
        @task
        def t_f(a=5):
            # type: (int)->Tuple[Optional[str]]
            # some comment
            assert a == 5
            return ("ok",)

        assert t_f.task.a.value_type.type == int
        assert t_f.task.result_1.value_type.type == str

    def test_union(self):
        @task
        def t_f(a=5):
            # type: (Union[int, str])->Union[str]
            # some comment
            assert a == 5
            return "ok"

        assert t_f.task.a.value_type.type == int
        assert t_f.task.result.value_type.type == str
