from dbnd._core.decorator.dbnd_func_proxy import DbndFuncProxy
from dbnd._core.tracking.python_tracking import (
    _is_function,
    track_functions,
    track_modules,
)
from test_dbnd.decorator import module_to_track
from test_dbnd.decorator.module_to_track import f1, f2, f3, f4, f6


class TestFunctionDecorating(object):
    def test_track_functions(self):
        assert _is_function(f1), "function got decorated unexpectedly"
        track_functions(f1, f2, f3)

        assert isinstance(f1, DbndFuncProxy), "local function wasn't decorated"
        assert isinstance(
            module_to_track.f1, DbndFuncProxy
        ), "function in module wasn't decorated"
        assert isinstance(f2, DbndFuncProxy)
        assert isinstance(f3, DbndFuncProxy)

    def test_track_modules(self):
        assert _is_function(f4), "function got decorated unexpectedly"
        track_modules(module_to_track)

        assert isinstance(f4, DbndFuncProxy), "local function wasn't decorated"
        assert isinstance(
            module_to_track.f4, DbndFuncProxy
        ), "function in module wasn't decorated"
        assert isinstance(module_to_track.f5, DbndFuncProxy)
        assert _is_function(module_to_track.task)

    def test_double_patching(self):
        track_functions(f6)
        track_functions(f6)
        track_modules(module_to_track)
        track_modules(module_to_track)
        assert _is_function(f6.func), "function was decorated more than once"
