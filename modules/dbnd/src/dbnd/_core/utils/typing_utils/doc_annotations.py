import inspect
import re

from typing import Callable, Dict, Iterable, Optional


TYPE_HINT_RE = re.compile(r"#type:\((.*)\)->(.+)")


class InvalidTypeHint(Exception):
    pass


# re:   Tuple[ Type1, Type2]
_TUPLE_TYPE_RE = re.compile(r"^Tuple\[(.+)]$")


def get_Tuple_params(type_str):
    tuple_def = _TUPLE_TYPE_RE.search(type_str)
    if tuple_def:
        return _split(tuple_def.group(1))
    return None


# re:   ( Type1, Type2,... )
_LIST_TYPE_RE = re.compile(r"^\((.+)\)$")


def get_inline_Tuple_params(type_str):
    list_type = _LIST_TYPE_RE.search(type_str)
    if list_type:
        return _split(list_type.group(1))
    return None


_SPLIT_ARGS_RE = re.compile(r",(?![^()\[\]]*[)\]])")


def _split(type_str):
    if not type_str:
        return []
    parts = _SPLIT_ARGS_RE.split(type_str)

    result = []
    current_part = parts.pop(0)
    while parts:
        if len(re.findall(r"\[", current_part)) > len(re.findall("]", current_part)):
            current_part += "," + parts.pop(0)
        else:
            result.append(current_part)
            current_part = parts.pop(0)
    result.append(current_part)
    return result


def _parse_return_type(return_types):
    # return type could be tuple as well
    # we would like to split this definition into "separate" types
    list_type = get_inline_Tuple_params(return_types)
    if list_type is not None:
        return list_type

    tuple_types = get_Tuple_params(return_types)
    if tuple_types is not None:
        return tuple_types
    return return_types


def _parse_python_2_type_hint_line(typehint_string):
    # type: (str) -> (tuple, type)
    #
    # we remove all whitespaces from the string
    # it simplifies our parsing

    typehint_string = typehint_string.replace(" ", "")
    search_results = TYPE_HINT_RE.search(typehint_string)
    if not search_results:
        raise InvalidTypeHint(
            "%s does not match type hint spec regex %s"
            % (typehint_string, TYPE_HINT_RE)
        )

    arg_types = _split(search_results.group(1))
    return_types = _parse_return_type(search_results.group(2))
    return arg_types, return_types


def parse_arg_types_for_callable(func):
    # type:(callable)->tuple
    """

    :param func:
    :return: list of parameter types if successfully parsed, else None
    """

    # todo make this compatible with python 3 type hints
    # python 2.7 type hint
    source_lines = inspect.getsource(func).split("\n")
    def_statements = 0
    for source_line in source_lines:
        try:
            arg_types_tuple, return_type = _parse_python_2_type_hint_line(source_line)
            return arg_types_tuple, return_type
        except InvalidTypeHint:
            if source_line.strip().startswith("def "):
                def_statements += 1
            if def_statements > 1:
                break

    return None, None


def get_doc_annotaions(f, arg_names):
    # type: (Callable, Iterable[str]) -> Optional[Dict[str,type ]]
    arg_types_tuple, return_type = parse_arg_types_for_callable(f)
    if arg_types_tuple or return_type:
        types = dict(zip(arg_names, arg_types_tuple))
        types["return"] = return_type
        return types
    return None
