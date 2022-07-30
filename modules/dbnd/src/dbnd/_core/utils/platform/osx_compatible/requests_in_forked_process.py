# © Copyright Databand.ai, an IBM Company 2022

import os

from sys import platform as _platform


is_osx = False
if _platform == "darwin":
    is_osx = True


def enable_osx_forked_request_calls():
    if not is_osx:
        return

    from dbnd._core.configuration.dbnd_config import config

    if not config.getboolean("core", "fix_env_on_osx"):
        return

    if "no_proxy" not in os.environ:
        os.environ["no_proxy"] = "*"

    if "OBJC_DISABLE_INITIALIZE_FORK_SAFETY" not in os.environ:
        os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "yes"
