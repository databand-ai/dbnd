import logging

import pytest
import six


if six.PY2:
    pytestmark = pytest.mark.skip("Python 2 non compatible code")  # py2 styling
else:
    from dbnd_examples.tutorial_syntax import T23_task_with_mutliple_outputs_py3 as T23

logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
    def test_t23_task_with_multiple_outputs_v1(self):
        t = T23.func_returns_two_dataframes_v1.dbnd_run(1).task
        assert t.features

    def test_t23_task_with_multiple_outputs_v2(self):
        t = T23.func_returns_two_dataframes_v2.dbnd_run(2).task
        assert t.features

    def test_t23_task_with_multiple_outputs_no_hint(self):
        t = T23.func_returns_two_dataframes_no_hint.dbnd_run(2).task
        assert t.result_1

    def test_t23_task_with_multiple_outputs_named_tuple_v1(self):
        t = T23.func_returns_two_dataframes_named_tuple_v1.dbnd_run(2).task
        assert t.features

    def test_t23_task_with_multiple_outputs_named_tuple_v2(self):
        t = T23.func_returns_two_dataframes_named_tuple_v2.dbnd_run(2).task
        assert t.features
