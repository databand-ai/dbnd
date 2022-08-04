# © Copyright Databand.ai, an IBM Company 2022

from collections import defaultdict

import six

from dbnd._vendor import namesgenerator
from dbnd._vendor.namesgenerator import get_random_name


EXPORTABLE_DICT = defaultdict(get_random_name)


def get_exportable_value(value):
    return EXPORTABLE_DICT[str(value)]


def get_hashed_name(s, paranoid=False):
    import hashlib

    if paranoid:
        return get_exportable_value(s)

    digest = hashlib.md5(str(s).encode()).digest()  # nosec B324
    if isinstance(digest, six.string_types):
        # python 2
        import struct

        digest = struct.unpack("16B", digest)

    a = digest[2] + 256 * digest[3]
    b = digest[0] + 256 * digest[1]
    name = "{}_{}".format(
        namesgenerator.left[a % len(namesgenerator.left)],
        namesgenerator.right[b % len(namesgenerator.right)],
    )

    return name
