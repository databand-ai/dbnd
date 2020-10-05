import inspect

import attr
import six

from dbnd._core.utils.basics.nothing import NOTHING, is_defined
from dbnd._core.utils.typing_utils.doc_annotations import get_doc_annotaions
from dbnd._vendor.pytypes.type_utils import get_Tuple_params, is_Tuple
from targets.values import get_types_registry


@attr.s
class _TaskDecoratorSpec(object):
    item = attr.ib()
    is_class = attr.ib()

    decorator_kwargs = attr.ib()
    default_result = attr.ib()

    # func spec
    args = attr.ib()
    varargs = attr.ib()
    varkw = attr.ib()
    defaults = attr.ib()
    kwonlyargs = attr.ib()
    kwonlydefaults = attr.ib()
    annotations = attr.ib()
    defaults_values = attr.ib()
    name = attr.ib()
    doc_annotations = attr.ib()


# Extracts arg types and return type from doc string
# # type: (str, dict) -> int
#          ^^^^^^^^^     ^^^

# Will split args type string to types (one level of nested commas supported)
# "str, dict(str, bool), list[str]" => ["str", "dict(str, bool)", "list[str]"]


def build_task_decorator_spec(class_or_func, decorator_kwargs, default_result):
    # type: (callable, ... ) -> _TaskDecoratorSpec
    is_class = inspect.isclass(class_or_func)
    if is_class:
        f = class_or_func.__init__
    else:
        f = class_or_func

    name = class_or_func.__name__
    if six.PY3:
        # python 3 https://docs.python.org/3/library/inspect.html#inspect.getfullargspec
        f_spec = inspect.getfullargspec(f)
        varkw = f_spec.varkw
        kwonlyargs = f_spec.kwonlyargs
        kwonlydefaults = f_spec.kwonlydefaults
        annotations = f_spec.annotations
    else:
        # python 2
        f_spec = inspect.getargspec(f)
        varkw = f_spec.keywords
        kwonlyargs = None
        kwonlydefaults = None
        annotations = None

    f_defaults = f_spec.defaults or {}
    defaults_start_at = len(f_spec.args) - len(f_defaults)

    return _TaskDecoratorSpec(
        item=class_or_func,
        is_class=is_class,
        decorator_kwargs=decorator_kwargs,
        default_result=default_result,
        args=f_spec.args,
        varargs=f_spec.varargs,
        varkw=varkw,
        defaults={
            f_spec.args[defaults_start_at + i]: v for i, v in enumerate(f_defaults)
        },
        kwonlyargs=kwonlyargs or [],
        kwonlydefaults=kwonlydefaults or {},
        annotations=annotations or {},
        defaults_values=f_defaults,
        name=name,
        doc_annotations=get_doc_annotaions(f, f_spec.args) or {},
    )


# 1. read default values provided and get their type - type(default_value), where exist
# 2. read py3 annotations if exist, and get args types
# 3. read doc string and get args types as str (doc string not necessary will compile)
# 4. for each one of the above - guess which Parameter type it should be. Currently supported:
#  a. bool/int/float/str -> all convert to relevant *Parameter type
#  b. DataFrame will convert to DataFrameParameter
#  c. collection types (currently only list, dict, tuple, set) won't convert to anything
#     (theoretically, it could be or *Param or TargetParameter)
#  d. if non of the above matched convert to Parameter
#  TODO: this logic should be certainly improved, even maybe give option to extend those conversions (someday)
# 5. If only single type "guessed" - use it. If more than one type (i.e doc str returned as_pickle,
#    but default_value returned float) - nothing happens, probably should raise exception. If 0 types
#    guessed - assume simple Parameter
def guess_func_arg_value_type(f_spec, name, default_value):
    # type: (_TaskDecoratorSpec, str, Any) -> ValueType
    r = get_types_registry()

    annotation = f_spec.annotations.get(name)
    if annotation is not None:
        return r.get_value_type_of_type(annotation)

    doc_annotation = f_spec.doc_annotations.get(name)
    t = r.get_value_type_of_type_str(doc_annotation)
    if t:
        return t

    if is_defined(default_value):
        return r.get_value_type_of_type(type(default_value))

    return None


def guess_func_return_type(f_spec):
    return_key = "return"

    r = get_types_registry()

    return_func_spec = f_spec.annotations.get(return_key, NOTHING)
    if is_defined(return_func_spec):
        if return_func_spec is None:
            return None

        # for -> (int,DataFrame)
        if isinstance(return_func_spec, tuple):
            return_func_spec = [
                ("result_%s" % idx, ret_part_type)
                for idx, ret_part_type in enumerate(return_func_spec, start=1)
            ]
        # easy way to check that it's NamedTuple
        elif hasattr(return_func_spec, "_field_types"):
            return_func_spec = list(six.iteritems(return_func_spec._field_types))
        # case of named tuple
        elif is_Tuple(return_func_spec):
            # for -> Tuple[int,DataFrame]
            return_func_spec = get_Tuple_params(return_func_spec)
            return_func_spec = [
                ("result_%s" % idx, ret_part_type)
                for idx, ret_part_type in enumerate(return_func_spec, start=1)
            ]

        if isinstance(return_func_spec, list):
            result = []
            for field_name, ret_part_type in return_func_spec:
                field_value_type = r.get_value_type_of_type(
                    ret_part_type, inline_value_type=True
                )
                result.append((field_name, field_value_type))
            return result
        else:
            # fallback to regular parsing
            return r.get_value_type_of_type(return_func_spec, inline_value_type=True)

    doc_annotation = f_spec.doc_annotations.get(return_key, NOTHING)
    if is_defined(doc_annotation):
        if doc_annotation == "None":
            return None
        # if it "return" , it parsed into list
        # doc_annotation
        if isinstance(doc_annotation, six.string_types):
            return r.get_value_type_of_type_str(doc_annotation) or NOTHING

        # so we have multiple params
        result = []
        for idx, ret_part_type in enumerate(doc_annotation, start=1):
            field_value_type = r.get_value_type_of_type_str(ret_part_type)
            result.append(("result_%s" % idx, field_value_type))
        return result
    return NOTHING


def args_to_kwargs(arg_names, args, kwargs):
    if not args:
        return args, kwargs

    # convert args to kwargs
    zipped = dict(zip(arg_names, args))
    # only if none of zipped keys passed in kwargs
    if all(k not in kwargs for k in zipped.keys()):
        n = len(zipped)
        args = args[n:]
        kwargs = kwargs.copy()
        kwargs.update(zipped)

    return args, kwargs
