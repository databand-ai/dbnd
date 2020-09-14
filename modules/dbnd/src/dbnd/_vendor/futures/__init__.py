# Copyright 2009 Brian Quinlan. All Rights Reserved.
# Licensed to PSF under a Contributor Agreement.


"""
Only for python 2.7 backwards compatibility.
In python 3, this package is already included in the standard library.
"""


"""Execute computations asynchronously using threads or processes."""

__author__ = 'Brian Quinlan (brian@sweetapp.com)'

from dbnd._vendor.futures._base import (FIRST_COMPLETED,
                                      FIRST_EXCEPTION,
                                      ALL_COMPLETED,
                                      CancelledError,
                                      TimeoutError,
                                      Future,
                                      Executor,
                                      wait,
                                      as_completed)
from dbnd._vendor.futures.thread import ThreadPoolExecutor

try:
    from dbnd._vendor.process import ProcessPoolExecutor
except ImportError:
    # some platforms don't have multiprocessing
    pass
