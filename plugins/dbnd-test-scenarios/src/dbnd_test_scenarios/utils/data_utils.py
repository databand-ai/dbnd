# © Copyright Databand.ai, an IBM Company 2022

import hashlib


def get_hash(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()  # nosec B324


def get_hash_for_obj(value: object) -> str:
    return hashlib.md5(str(value).encode()).hexdigest()  # nosec B324


def dict_to_str(d):
    return " ".join(["%s:%s" % (key, value) for key, value in d.items()])
