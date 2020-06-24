import os

from dbnd import get_databand_context
from dbnd.tasks.py_distribution.build_distribution import (
    build_fat_requirements_py_zip_file,
)
from dbnd.tasks.py_distribution.fat_wheel_config import FatWheelConfig


_bdist_zip_cache = {}


def build_fat_wheel(use_cached=True, output_dir=None):
    output_dir = output_dir or _get_output_dir(use_cached=use_cached)
    conf = FatWheelConfig()
    bdist_zip_cache_key = (conf.package_dir, conf.requirements_file)

    if (
        use_cached
        and os.path.exists(output_dir.path)
        and bdist_zip_cache_key in _bdist_zip_cache
    ):
        return _bdist_zip_cache[bdist_zip_cache_key]

    result_file = build_fat_requirements_py_zip_file(
        output_dir, conf.package_dir, conf.requirements_file
    )
    _bdist_zip_cache[bdist_zip_cache_key] = result_file
    return result_file


def _get_output_dir(use_cached):
    dc = get_databand_context()
    working_dir = dc.env.dbnd_local_root__build
    if use_cached:
        working_dir = working_dir.folder("bdist_zip_build_%s" % dc.current_context_uid)
    else:
        import random

        random_number = random.randrange(0, 1000)
        working_dir = working_dir.folder(
            "bdist_zip_build_%s_%d" % (dc.current_context_uid, random_number)
        )
    return working_dir
