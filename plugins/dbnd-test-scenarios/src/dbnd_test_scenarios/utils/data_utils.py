import hashlib


def get_hash(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()


def dict_to_str(d):
    return " ".join(["%s:%s" % (key, value) for key, value in d.items()])
