from dbnd._core.tracking.python_tracking import (
    _is_function,
    _is_task,
    track_functions,
    track_modules,
)
from test_dbnd.decorator import module_to_track
from test_dbnd.decorator.module_to_track import f1, f2, f3, f4, f6


class TestFunctionDecorating(object):
    def test_track_functions(self):
        assert _is_function(f1), "function got decorated unexpectedly"
        assert not _is_task(f1), "function got decorated unexpectedly"
        track_functions(f1, f2, f3)

        assert _is_function(f1), "function is not function anymore"
        assert _is_task(f1), "local function wasn't decorated"
        assert _is_task(module_to_track.f1), "function in module wasn't decorated"
        assert _is_task(f2)
        assert _is_task(f3)

    def test_track_modules(self):
        assert _is_function(f4), "function got decorated unexpectedly"
        assert not _is_task(f4), "function got decorated unexpectedly"
        track_modules(module_to_track)

        assert _is_function(f4), "function is not function anymore"
        assert _is_task(f4), "local function wasn't decorated"
        assert _is_function(module_to_track.f4), "function is not function anymore"
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
        assert _is_function(f6.func), "function was decorated more than once"
