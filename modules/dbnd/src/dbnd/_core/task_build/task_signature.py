import hashlib
import re

from collections import namedtuple

from dbnd._core.utils import json_utils
from dbnd._core.utils.traversing import flatten, traverse_frozen_set


SIGNATURE_ID_TRUNCATE_HASH = 10

TASK_ID_INCLUDE_PARAMS = 3
TASK_ID_TRUNCATE_PARAMS = 16
TASK_ID_INVALID_CHAR_REGEX = re.compile(r"[^A-Za-z0-9_.]")

Signature = namedtuple("Signature", ["id", "signature", "signature_source"])


def task_params_short(params):
    param_summary = "_".join(
        p[:TASK_ID_TRUNCATE_PARAMS]
        for p in (params[p] for p in sorted(params)[:TASK_ID_INCLUDE_PARAMS])
    )
    param_summary = TASK_ID_INVALID_CHAR_REGEX.sub("_", param_summary)

    return param_summary


def build_signature(name, params, extra=None):
    """
    Returns a canonical string used to identify a particular task

    :param name:
    :param params: a list mapping parameter names to their serialized values
    :return: A unique, shortened identifier corresponding to the family and params
    """
    # task_id is a concatenation of task family,
    # sorted by parameter name and a md5hash of the family/parameters as a cananocalised json.

    params = {key: value for key, value in params}

    signature_dict = {"name": name, "params": params}

    if extra:
        signature_dict["extra"] = extra

    # we can't handle sets
    signature_dict = traverse_frozen_set(signature_dict)
    param_str = json_utils.dumps_canonical(signature_dict)

    signature = user_friendly_signature(param_str)
    task_id = "{}__{}".format(name, signature)

    return Signature(id=task_id, signature=signature, signature_source=param_str)


def build_signature_from_values(name, struct):
    values = set([str(value) for value in flatten(struct)])
    signature_str = "|".join(sorted(values))
    signature = hashlib.md5(signature_str.encode("utf-8")).hexdigest()
    return Signature(id=name, signature=signature, signature_source=signature_str)


def user_friendly_signature(str_value):
    hash_md5 = hashlib.md5(str_value.encode("utf-8")).hexdigest()

    return hash_md5[:SIGNATURE_ID_TRUNCATE_HASH]


def build_user_friendly_signature_from_values(name, struct):
    sig = build_signature_from_values(name, struct)

    return sig.signature[:SIGNATURE_ID_TRUNCATE_HASH]
