from __future__ import absolute_import
"""
Copyright 2018 Steven Loria

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

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
