# -*- coding: utf-8 -*-
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

from .schema import (
    Schema,
    SchemaOpts,
    MarshalResult,
    UnmarshalResult,
)
from . import fields
from .decorators import (
    pre_dump, post_dump, pre_load, post_load, validates, validates_schema
)
from .utils import pprint, missing
from .exceptions import ValidationError
from distutils.version import LooseVersion

__version__ = '2.18.0'
__version_info__ = tuple(LooseVersion(__version__).version)
__author__ = 'Steven Loria'
__all__ = [
    'Schema',
    'SchemaOpts',
    'fields',
    'validates',
    'validates_schema',
    'pre_dump',
    'post_dump',
    'pre_load',
    'post_load',
    'pprint',
    'MarshalResult',
    'UnmarshalResult',
    'ValidationError',
    'missing',
]
