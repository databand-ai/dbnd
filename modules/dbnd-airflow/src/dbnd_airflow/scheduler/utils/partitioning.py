# © Copyright Databand.ai, an IBM Company 2022

from typing import Tuple


def partition_from_module_name(
    module_name: str, base_filename: str = "dbnd_dags_from_databand_"
) -> Tuple[int, int]:
    index = module_name.find(base_filename)
    parts = module_name[index + len(base_filename) :]
    result = parts.split("__")
    return int(result[0]), int(result[1])
