import contextlib
import logging
import os
import shutil
import subprocess
import sys
import zipfile

from tempfile import mkdtemp

from dbnd._core.current import is_verbose


logger = logging.getLogger(__name__)


def _get_package_name_and_version_from_whl(whl_dir):
    all_files = os.listdir(whl_dir)
    if not all_files:
        raise Exception("No Whl files where created")

    if len(all_files) > 1:
        raise Exception("Directory has more than one whl file")

    whl_file = all_files[0]
    split_name = whl_file.split("-")
    return split_name[0], split_name[1]


def _run_command(generate_command):
    if subprocess.call(generate_command, shell=True) != 0:
        raise Exception("Failed running {} command".format(generate_command))


@contextlib.contextmanager
def _create_temp_working_dir(tmp_build_dir=None):
    clean_build_dir = False
    try:
        if not tmp_build_dir:
            tmp_build_dir = mkdtemp(prefix="dbnd-build-")
            clean_build_dir = True
        yield tmp_build_dir
    finally:
        if clean_build_dir:
            if is_verbose():  # do not clean build dir in verbose mode
                logger.info("Keeping build dir because verbose mode is on")
            else:
                logger.info("Deleting tmp directory: %s", tmp_build_dir)
                shutil.rmtree(tmp_build_dir, ignore_errors=True)


def generate_project_whl(package_dir, output_dir):
    # generating deps wheels

    # Very important to change to the working dir, otherwise, wheel creation won't work as expected

    curdir = os.curdir
    setup_py_path = os.path.join(package_dir, "setup.py")
    if not os.path.exists(setup_py_path):
        raise Exception(
            "Can't find setup.py inside package dir {} (curdir={})".format(
                package_dir, curdir
            )
        )

    # we are going to be inside the package dir (this way pip works correctly)
    generate_command = "{} {} bdist_wheel --dist-dir {}".format(
        sys.executable, "setup.py", output_dir
    )

    try:
        os.chdir(package_dir)
        _run_command(generate_command)
    finally:
        os.chdir(curdir)


def generate_third_party_deps(requirements_file, output_dir):
    # generating deps wheels
    generate_command = "{} -m pip wheel -r {} -w {}".format(
        sys.executable, requirements_file, output_dir
    )

    _run_command(generate_command)


def zip_dir(zip_file_path, source_dir_path):
    result_zip = zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED)

    for dir_name, _, files in os.walk(source_dir_path):
        if dir_name == source_dir_path:
            for file_name in files:
                full_file_name = os.path.join(dir_name, file_name)
                temp_zip = zipfile.ZipFile(file=full_file_name, mode="r")
                for file_info in temp_zip.filelist:
                    file_content = temp_zip.read(file_info.filename)
                    result_zip.writestr(file_info, file_content)
    result_zip.close()
