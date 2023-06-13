# © Copyright Databand.ai, an IBM Company 2022

from typing import List

from dbnd._core.configuration.dbnd_config import DbndConfig
from dbnd._core.utils.basics.load_python_module import load_python_module


def load_user_modules(dbnd_config: DbndConfig, modules: List[str] = None) -> None:
    # loading user modules
    module_from_config = dbnd_config.get("run", "module")
    if module_from_config:
        load_python_module(module_from_config, "config file (see [databand].module)")
    if modules:
        for m in modules:
            load_python_module(m, "--module")
