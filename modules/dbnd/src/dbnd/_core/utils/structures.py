import logging

import six


logger = logging.getLogger(__file__)


def inverse_dict(x):
    """ Make keys or values and values of keys """
    return {v: k for k, v in six.iteritems(x)}


def list_to_tuple(x):
    """ Make tuples out of lists and sets to allow hashing """
    if isinstance(x, list) or isinstance(x, set):
        return tuple(x)
    else:
        return x


class MappingCombineStrategy(object):
    replace_and_warn = "replace_and_warn"
    replace = "replace"


def combine_mappings(left, right, strategy=MappingCombineStrategy.replace):
    left = left or {}
    right = right or {}

    # immidiate return if similar of empty
    if not left:
        return right.copy()

    result = left.copy()
    if left == right or not right:
        return result

    for key, value in six.iteritems(right):
        if strategy == MappingCombineStrategy.replace_and_warn and key in result:
            logger.warning("Overriding '%s' value with '%s'", key, value)

        result[key] = value
    return result


def split_list(func, iter):
    left, right = [], []
    for v in iter:
        if func(v):
            left.append(v)
        else:
            right.append(v)
    return left, right


def list_of_strings(value_list):
    if not value_list:
        return []
    return list(map(str, value_list))


def get_object_attrs(cls):
    params = []
    for param_name in dir(cls):
        param_obj = getattr(cls, param_name)
        params.append((param_name, param_obj))
    return params
