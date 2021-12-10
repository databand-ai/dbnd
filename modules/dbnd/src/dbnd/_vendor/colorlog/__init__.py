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

import sys
import warnings

from .formatter import (
    ColoredFormatter,
    LevelFormatter,
    TTYColoredFormatter,
    default_log_colors,
)
from .wrappers import (
    CRITICAL,
    DEBUG,
    ERROR,
    FATAL,
    INFO,
    NOTSET,
    StreamHandler,
    WARN,
    WARNING,
    basicConfig,
    critical,
    debug,
    error,
    exception,
    getLogger,
    info,
    log,
    root,
    warning,
)

__all__ = (
    "CRITICAL",
    "DEBUG",
    "ERROR",
    "FATAL",
    "INFO",
    "NOTSET",
    "WARN",
    "WARNING",
    "ColoredFormatter",
    "LevelFormatter",
    "StreamHandler",
    "TTYColoredFormatter",
    "basicConfig",
    "critical",
    "debug",
    "default_log_colors",
    "error",
    "exception",
    "exception",
    "getLogger",
    "info",
    "log",
    "root",
    "warning",
)

if sys.version_info < (3, 6):
    warnings.warn(
        "Colorlog requires Python 3.6 or above. Pin 'colorlog<5' to your dependencies "
        "if you require compatibility with older versions of Python. See "
        "https://github.com/borntyping/python-colorlog#status for more information."
    )
