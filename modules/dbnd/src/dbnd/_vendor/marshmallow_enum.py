# COMMIT 1c62db5
"""
Vendorized from https://github.com/justanr/marshmallow_enum/releases/tag/v1.5.1
The MIT License (MIT)

Copyright (c) 2015 Alec Nikolas Reiter

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
from __future__ import unicode_literals

import sys
import warnings

from enum import Enum

from dbnd._vendor.marshmallow import ValidationError, fields


PY2 = sys.version_info.major == 2
# ugh Python 2
if PY2:
    string_types = (str, unicode)  # noqa: F821
    text_type = unicode  # noqa: F821
else:
    string_types = (str,)
    text_type = str


class LoadDumpOptions(Enum):
    value = 1
    name = 0


class EnumField(fields.Field):
    VALUE = LoadDumpOptions.value
    NAME = LoadDumpOptions.name

    default_error_messages = {
        "by_name": "Invalid enum member {input}",
        "by_value": "Invalid enum value {input}",
        "must_be_string": "Enum name must be string",
    }

    def __init__(
        self,
        enum,
        by_value=False,
        load_by=None,
        dump_by=None,
        error="",
        *args,
        **kwargs
    ):
        self.enum = enum
        self.by_value = by_value

        if error and any(old in error for old in ("{name", "{value", "{choices")):
            warnings.warn(
                "'name', 'value', and 'choices' fail inputs are deprecated,"
                "use input, names and values instead",
                DeprecationWarning,
                stacklevel=2,
            )

        self.error = error

        if load_by is None:
            load_by = LoadDumpOptions.value if by_value else LoadDumpOptions.name

        if load_by not in LoadDumpOptions:
            raise ValueError(
                "Invalid selection for load_by must be EnumField.VALUE or EnumField.NAME, got {}".format(
                    load_by
                )
            )

        if dump_by is None:
            dump_by = LoadDumpOptions.value if by_value else LoadDumpOptions.name

        if dump_by not in LoadDumpOptions:
            raise ValueError(
                "Invalid selection for load_by must be EnumField.VALUE or EnumField.NAME, got {}".format(
                    dump_by
                )
            )

        self.load_by = load_by
        self.dump_by = dump_by

        super(EnumField, self).__init__(*args, **kwargs)

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        elif self.dump_by == LoadDumpOptions.value:
            return value.value
        else:
            return value.name

    def _deserialize(self, value, attr, data):
        if value is None:
            return None
        elif self.load_by == LoadDumpOptions.value:
            return self._deserialize_by_value(value, attr, data)
        else:
            return self._deserialize_by_name(value, attr, data)

    def _deserialize_by_value(self, value, attr, data):
        try:
            return self.enum(value)
        except ValueError:
            self.fail("by_value", input=value, value=value)

    def _deserialize_by_name(self, value, attr, data):
        if not isinstance(value, string_types):
            self.fail("must_be_string", input=value, name=value)

        try:
            return getattr(self.enum, value)
        except AttributeError:
            self.fail("by_name", input=value, name=value)

    def fail(self, key, **kwargs):
        kwargs["values"] = ", ".join([text_type(mem.value) for mem in self.enum])
        kwargs["names"] = ", ".join([mem.name for mem in self.enum])

        if self.error:
            if self.by_value:
                kwargs["choices"] = kwargs["values"]
            else:
                kwargs["choices"] = kwargs["names"]
            msg = self.error.format(**kwargs)
            raise ValidationError(msg)
        else:
            super(EnumField, self).fail(key, **kwargs)
