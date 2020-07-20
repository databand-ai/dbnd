from typing import List

from dbnd import task
from dbnd.tasks.py_distribution.fat_wheel_builder import (
    build_fat_wheel,
    build_wheel_zips,
)


@task
def fat_wheel_building_task(use_cached=True, output_dir=None):
    # type: (bool, str) -> str
    return build_fat_wheel(use_cached, output_dir)


@task
def wheel_zips_building_task(use_cached=True, output_dir=None):
    # type: (bool, str) -> List[str]
    return build_wheel_zips(use_cached, output_dir)
