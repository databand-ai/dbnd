# © Copyright Databand.ai, an IBM Company 2022

import contextlib

from dbnd._core.utils.basics.nested_context import safe_nested


class FaultyContextManagerOnEnter:
    def __enter__(self):
        raise Exception("faulty_context_manager_on_enter error on __enter__")

    def __exit__(self, exc_type, exc_value, traceback):
        pass


@contextlib.contextmanager
def empty_context_manager():
    yield


@contextlib.contextmanager
def faulty_context_manager_on_enter():
    raise Exception("faulty_context_manager_on_enter error on __enter__")


@contextlib.contextmanager
def faulty_context_manager_on_exit():
    yield
    raise Exception("faulty_context_manager_on_exit error on __exit__")


@contextlib.contextmanager
def multiple_ctx_with_error_in_body():
    with safe_nested(
        empty_context_manager(), empty_context_manager(), empty_context_manager()
    ):
        try:
            raise Exception("multiple_ctx_with_error_in_body error")
        finally:
            yield


@contextlib.contextmanager
def multiple_context_managers_with_errors():
    with safe_nested(
        FaultyContextManagerOnEnter(),
        faulty_context_manager_on_exit(),
        faulty_context_manager_on_exit(),
    ):
        yield


def test_safe_nested_mutes_on_enter():
    try:
        with safe_nested(multiple_context_managers_with_errors()):
            pass
    except Exception:
        assert False, "should not be here, we do not expect exceptions here"


def test_safe_nested_does_not_swallow_exception_inside_wrapped_operator():
    try:
        with multiple_ctx_with_error_in_body():
            pass
    except Exception as e:
        assert e.args[0] == "multiple_ctx_with_error_in_body error"
        return

    assert False, "should not be here, exception must be raised"
