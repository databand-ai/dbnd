import os

from dbnd import get_databand_context
from dbnd.tasks.py_distribution.build_distribution import (
    build_fat_requirements_py_zip_file,
    build_package_zip,
    build_third_party_zip,
)
from dbnd.tasks.py_distribution.fat_wheel_config import FatWheelConfig


_bdist_zip_cache = {}
_bdist_zip_list_cache = {}


def build_fat_wheel(use_cached=True, output_dir=None):
    """
    Builds wheel files for package and third party requirements (if given)
    and merges them into one big wheel file (with zip suffix).
    Returns the path to the created zip file.
    """
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


def build_wheel_zips(use_cached=True, output_dir=None):
    """
    Builds one wheel file for the package (with zip suffix) and one wheel file
    for all third party requirements (zip suffix as well).
    Returns a list that contains the paths of the two zip files.
    """
    output_dir = output_dir or _get_output_dir(use_cached=use_cached)
    conf = FatWheelConfig()
    bdist_zip_cache_key = (conf.package_dir, conf.requirements_file)
    if (
        use_cached
        and os.path.exists(output_dir.path)
        and bdist_zip_cache_key in _bdist_zip_cache
    ):
        return _bdist_zip_list_cache[bdist_zip_cache_key]

    package_zip = build_package_zip(output_dir, conf.package_dir)
    result_file_list = [package_zip]
    if conf.requirements_file:
        third_party_zip = build_third_party_zip(output_dir, conf.requirements_file)
        result_file_list.append(third_party_zip)
    _bdist_zip_list_cache[bdist_zip_cache_key] = result_file_list
    return result_file_list


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
