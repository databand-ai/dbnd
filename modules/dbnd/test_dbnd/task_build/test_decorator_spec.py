# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.utils.callable_spec import build_callable_spec


class TestTaskDecoratorSpec(object):
    def test_only_args(self):
        def only_arguments(a, b, c):
            pass

        decorator_spec = build_callable_spec(only_arguments)
        assert not decorator_spec.is_class
        assert decorator_spec.args == ["a", "b", "c"]
        assert decorator_spec.varargs is None
        assert decorator_spec.varkw is None
        assert decorator_spec.defaults == {}
        assert decorator_spec.kwonlyargs == []
        assert decorator_spec.kwonlydefaults == {}
        assert decorator_spec.defaults_values == ()

    def test_arguments_and_kwargs(self):
        def arguments_and_kwargs(a, b, c, **kwargs):
            pass

        decorator_spec = build_callable_spec(arguments_and_kwargs)
        assert not decorator_spec.is_class
        assert decorator_spec.args == ["a", "b", "c"]
        assert decorator_spec.varargs is None
        assert decorator_spec.varkw == "kwargs"
        assert decorator_spec.defaults == {}
        assert decorator_spec.kwonlyargs == []
        assert decorator_spec.kwonlydefaults == {}
        assert decorator_spec.defaults_values == ()

    def test_arguments_and_named_kwarg(self):
        def arguments_and_named_kwarg(a, b, c, kwarg="default"):
            pass

        decorator_spec = build_callable_spec(arguments_and_named_kwarg)
        assert not decorator_spec.is_class
        assert decorator_spec.args == ["a", "b", "c", "kwarg"]
        assert decorator_spec.varargs is None
        assert decorator_spec.varkw is None
        assert decorator_spec.defaults == {"kwarg": "default"}
        assert decorator_spec.kwonlyargs == []
        assert decorator_spec.kwonlydefaults == {}
        assert decorator_spec.defaults_values == ("default",)

    def test_named_kwarg_only(self):
        def named_kwarg_and_vkwarg(a=1, b=2, c=3, kwarg="default", **kwargs):
            pass

        decorator_spec = build_callable_spec(named_kwarg_and_vkwarg)
        assert not decorator_spec.is_class
        assert decorator_spec.args == ["a", "b", "c", "kwarg"]
        assert decorator_spec.varargs is None
        assert decorator_spec.varkw == "kwargs"
        assert decorator_spec.defaults == {"a": 1, "b": 2, "c": 3, "kwarg": "default"}
        assert decorator_spec.kwonlyargs == []
        assert decorator_spec.kwonlydefaults == {}
        assert decorator_spec.defaults_values == (1, 2, 3, "default")

    def test_doc_annotations(self):
        def with_doc_annotations(a, b, **kwargs):
            # type: (int, str, **str) -> int
            pass

        decorator_spec = build_callable_spec(with_doc_annotations)
        assert decorator_spec.annotations == {}
        assert decorator_spec.doc_annotations == {
            "a": "int",
            "b": "str",
            "return": "int",
        }
