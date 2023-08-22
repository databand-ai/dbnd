# © Copyright Databand.ai, an IBM Company 2022
import re

from dbnd import track_scope_functions
from dbnd._core.tracking.python_tracking import (
    _is_function,
    _is_task,
    track_functions,
    track_modules,
)
from test_dbnd.tracking.callable_tracking import module_to_track
from test_dbnd.tracking.callable_tracking.module_to_track import f1, f2, f3, f4, f6


def t_no_decorator1():
    pass


def t_no_decorator2():
    pass


def t_no_decorator3():
    pass


def t_no_decorator4():
    pass


def tr_egexp():
    pass


def wrong_prefix_t_no_decorator1():
    pass


class TestFunctionDecorating(object):
    def test_track_functions(self):
        assert _is_function(f1), "function got decorated unexpectedly"
        assert not _is_task(f1), "function got decorated unexpectedly"
        track_functions(f1, f2, f3)

        assert callable(f1), "function is not function anymore"
        assert _is_task(f1), "local function wasn't decorated"
        assert _is_task(module_to_track.f1), "function in module wasn't decorated"
        assert _is_task(f2)
        assert _is_task(f3)

    def test_track_modules(self):
        assert _is_function(f4), "function got decorated unexpectedly"
        assert not _is_task(f4), "function got decorated unexpectedly"
        track_modules(module_to_track)

        assert callable(f4), "function is not function anymore"
        assert _is_task(f4), "local function wasn't decorated"
        assert callable(module_to_track.f4), "function is not function anymore"
        assert _is_task(module_to_track.f4), "function in module wasn't decorated"
        assert _is_task(module_to_track.f5)
        assert _is_function(module_to_track.task)
        assert not _is_task(module_to_track.task)

    def test_double_patching(self):
        id_before_track = id(f6)
        track_functions(f6)
        id_after_track = id(f6)
        assert id_before_track == id_after_track
        track_functions(f6)
        # second track shouldn't change
        assert id_before_track == id(f6)

        track_modules(module_to_track)
        track_modules(module_to_track)
        assert callable(f6.callable), "function was decorated more than once"

    def test_track_scope_functions_simple(self):

        track_scope_functions(
            prefixes=["t_"], scope=globals(), module=t_no_decorator1.__module__
        )

        assert callable(t_no_decorator1), "function is not function anymore"
        assert _is_task(t_no_decorator1), "local function wasn't decorated"
        assert callable(t_no_decorator2), "function is not function anymore"
        assert _is_task(t_no_decorator2), "function in module wasn't decorated"

        assert not _is_task(tr_egexp), "function in module decorated"

        track_scope_functions(
            prefixes=[re.compile(r"t[r]_")],
            scope=globals(),
            module=t_no_decorator1.__module__,
        )

        assert callable(tr_egexp), "function is not function anymore"
        assert _is_task(tr_egexp), "function in module wasn't decorated"
