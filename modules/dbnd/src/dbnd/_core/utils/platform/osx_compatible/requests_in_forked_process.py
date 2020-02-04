import os

from sys import platform as _platform


is_osx = False
if _platform == "darwin":
    is_osx = True


def enable_osx_forked_request_calls():
    if not is_osx:
        return

    from dbnd._core.configuration.dbnd_config import config

    if not config.getboolean("core", "fix_requests_on_osx"):
        return

    if "no_proxy" not in os.environ:
        os.environ["no_proxy"] = "*"
