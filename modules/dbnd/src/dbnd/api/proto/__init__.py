# The moment we access this module, we need to find out if we need to use vendored protobuf package

import logging
import os
import sys


logger = logging.getLogger(__name__)

try:
    from google import protobuf

    logger.info(
        "Using `google.protobuf` package that is available in current python env"
    )
    if protobuf.__version__ != "3.13.0":
        logger.warn(
            "Excpected `protobuf==3.13.0` but %s is installed in current python env",
            protobuf.__version__,
        )
except ImportError:
    from dbnd import _vendor_package

    vendor_package_str = os.path.dirname(_vendor_package.__file__)

    sys.path.append(vendor_package_str)
    logger.info(
        "Using vendored `google.protobuf` package as it's not available in current python env"
    )
