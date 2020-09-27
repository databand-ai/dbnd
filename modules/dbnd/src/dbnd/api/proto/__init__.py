# The moment we access this module, we need to find out if we need to use vendored protobuf package

import logging
import os
import sys

import dbnd


logger = logging.getLogger(__name__)


def is_google_installed():
    try:
        import google

        return True
    except ImportError:
        return False


def is_potobuf_installed():
    try:
        from google import protobuf

        return True
    except ImportError:
        return False


def add_vendored_package_to_syspath():
    vendor_package_str = os.path.join(os.path.dirname(dbnd.__file__), "_vendor_package")
    sys.path.append(vendor_package_str)
    logger.info("Adding `dbnd/_vendor_package/` to `sys.path`.")
    assert is_potobuf_installed()


if is_google_installed():
    from dbnd._vendor_package.google import protobuf as vendored_protobuf

    if is_potobuf_installed():
        from google import protobuf

        logger.info(
            "Using `google.protobuf` package that is available in the current python env"
        )

        if protobuf.__version__ != vendored_protobuf.__version__:
            logger.warn(
                "Excpected `protobuf==%s` but %s is installed in the current python env",
                vendored_protobuf.__version__,
                protobuf.__version__,
            )
    else:
        raise ImportError(
            "Unable to use vendored protobuf package while there are other google packages installed. "
            "Please use `pip install protobuf==3.13.0` to proceed."
        )
else:
    if dbnd.config.getboolean("core", "allow_vendored_package"):
        logger.info("Missing `google.protobuf` package in the current python env.")
        add_vendored_package_to_syspath()
    else:
        raise ImportError(
            "Missing `google.protobuf` package in the current python env while "
            "core.allow_vendored_package=False"
        )
