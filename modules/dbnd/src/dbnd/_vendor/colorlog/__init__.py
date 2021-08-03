
""" VENDORIZED python-colorlog
    https://github.com/borntyping/python-colorlog
    version 4.1.0
The MIT License (MIT)

Copyright (c) 2018 Sam Clements <sam@borntyping.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.



  A logging formatter for colored output."""

from __future__ import absolute_import

from .colorlog import (
    escape_codes, default_log_colors,
    ColoredFormatter, LevelFormatter, TTYColoredFormatter)

from .logging import (
    basicConfig, root, getLogger, log,
    debug, info, warning, error, exception, critical, StreamHandler)

__all__ = ('ColoredFormatter', 'default_log_colors', 'escape_codes',
           'basicConfig', 'root', 'getLogger', 'debug', 'info', 'warning',
           'error', 'exception', 'critical', 'log', 'exception',
           'StreamHandler', 'LevelFormatter', 'TTYColoredFormatter')
