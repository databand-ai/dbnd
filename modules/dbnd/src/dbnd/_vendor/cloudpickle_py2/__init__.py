from __future__ import absolute_import

import sys
import pickle


from dbnd._vendor.cloudpickle_py2.cloudpickle import *
if sys.version_info[:2] >= (3, 8):
    from dbnd._vendor.cloudpickle_py2.cloudpickle_fast import CloudPickler, dumps, dump

__version__ = '1.2.2'
