from __future__ import absolute_import

from distutils.version import LooseVersion


try:
    import marshmallow

    version_tuple = tuple(LooseVersion(marshmallow.__version__).version)
    if version_tuple != (2, 18, 0):
        raise ImportError

    from marshmallow import (
        base,
        class_registry,
        decorators,
        exceptions,
        fields,
        marshalling,
        orderedset,
        schema,
        utils,
        validate,
        warnings,
        warnings,
    )
    from marshmallow.schema import Schema, SchemaOpts, MarshalResult, UnmarshalResult
    from marshmallow.decorators import (
        pre_dump,
        post_dump,
        pre_load,
        post_load,
        validates,
        validates_schema,
    )
    from marshmallow.utils import pprint, missing
    from marshmallow.exceptions import ValidationError

except ImportError:
    from dbnd._vendor import _marshmallow as marshmallow

    from dbnd._vendor._marshmallow import (
        base,
        class_registry,
        decorators,
        exceptions,
        fields,
        marshalling,
        orderedset,
        schema,
        utils,
        validate,
        warnings,
        warnings,
    )
    from dbnd._vendor._marshmallow.schema import (
        Schema,
        SchemaOpts,
        MarshalResult,
        UnmarshalResult,
    )
    from dbnd._vendor._marshmallow.decorators import (
        pre_dump,
        post_dump,
        pre_load,
        post_load,
        validates,
        validates_schema,
    )
    from dbnd._vendor._marshmallow.utils import pprint, missing
    from dbnd._vendor._marshmallow.exceptions import ValidationError

__version__ = marshmallow.__version__
__version_info__ = marshmallow.__version_info__
__author__ = marshmallow.__author__

__all__ = [
    "Schema",
    "SchemaOpts",
    "fields",
    "validates",
    "validates_schema",
    "pre_dump",
    "post_dump",
    "pre_load",
    "post_load",
    "pprint",
    "MarshalResult",
    "UnmarshalResult",
    "ValidationError",
    "missing",
]
