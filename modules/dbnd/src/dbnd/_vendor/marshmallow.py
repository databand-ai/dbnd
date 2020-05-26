from __future__ import absolute_import

from distutils.version import LooseVersion


try:
    import marshmallow

    version_tuple = tuple(LooseVersion(marshmallow.__version__).version)
    if version_tuple != (2, 18, 0):
        raise ImportError

    from marshmallow import *
    from marshmallow import validate
    from marshmallow import fields
except ImportError:
    from dbnd._vendor._marshmallow import *
    from dbnd._vendor._marshmallow import validate
    from dbnd._vendor._marshmallow import fields
