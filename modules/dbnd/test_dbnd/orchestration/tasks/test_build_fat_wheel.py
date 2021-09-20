import os
import zipfile

import pytest
import six

from dbnd import dbnd_config
from dbnd.tasks.py_distribution.fat_wheel_builder import (
    build_fat_wheel,
    build_wheel_zips,
)
from dbnd_test_scenarios.scenarios_repo import test_scenario_path


class TestBuildFatWheel(object):
    @pytest.mark.skipif(not six.PY3, reason="requires python3, due to pathlib")
    def test_fat_wheel_task(self):
        from dbnd.tasks.py_distribution.fat_wheel_tasks import ProjectWheelFile

        with dbnd_config(
            {
                "ProjectWheelFile": {
                    "package_dir": test_scenario_path("dbnd-test-package"),
                    "requirements_file": test_scenario_path(
                        "dbnd-test-package/requirements.txt"
                    ),
                }
            }
        ):
            fat_wheel_task = ProjectWheelFile.build_project_wheel_file_task()
            fat_wheel_task.dbnd_run()
            wheel_file = fat_wheel_task.wheel_file
            assert os.path.exists(wheel_file)

            temp_zip = zipfile.ZipFile(file=wheel_file, mode="r")
            all_files = temp_zip.NameToInfo.keys()

            assert "six.py" in all_files
            assert "dbnd_test_package/my_lib.py" in all_files
            assert "dbnd_test_package-0.1.dist-info/METADATA" in all_files
            assert "luigi/task.py" in all_files

    @pytest.mark.skipif(not six.PY3, reason="requires python3, due to pathlib")
    def test_build_fat_wheel(self):
        with dbnd_config(
            {
                "bdist_zip": {
                    "package_dir": test_scenario_path("dbnd-test-package"),
                    "requirements_file": test_scenario_path(
                        "dbnd-test-package/requirements.txt"
                    ),
                }
            }
        ):
            bdist_file = build_fat_wheel()
            assert os.path.exists(bdist_file)

            temp_zip = zipfile.ZipFile(file=bdist_file, mode="r")
            all_files = temp_zip.NameToInfo.keys()

            assert "six.py" in all_files
            assert "dbnd_test_package/my_lib.py" in all_files
            assert "dbnd_test_package-0.1.dist-info/METADATA" in all_files
            assert "luigi/task.py" in all_files

            # check cache
            new_bdist_file = build_fat_wheel()
            assert bdist_file == new_bdist_file

    @pytest.mark.skipif(not six.PY3, reason="requires python3, due to pathlib")
    def test_build_separate_wheels(self):
        with dbnd_config(
            {
                "bdist_zip": {
                    "package_dir": test_scenario_path("dbnd-test-package"),
                    "requirements_file": test_scenario_path(
                        "dbnd-test-package/requirements.txt"
                    ),
                }
            }
        ):
            zip_files = build_wheel_zips()
            assert len(zip_files) == 2

            package_zip = (
                zip_files[0]
                if zip_files[1].endswith("third-party-deps.zip")
                else zip_files[1]
            )
            third_patry_zip = (
                zip_files[1] if zip_files[0] == package_zip else zip_files[0]
            )

            assert os.path.exists(package_zip)
            assert os.path.exists(third_patry_zip)

            package_zip_file = zipfile.ZipFile(file=package_zip, mode="r")
            all_package_files = package_zip_file.NameToInfo.keys()

            assert "dbnd_test_package/my_lib.py" in all_package_files
            assert "dbnd_test_package-0.1.dist-info/METADATA" in all_package_files

            third_patry_zip_file = zipfile.ZipFile(file=third_patry_zip, mode="r")
            all_third_patry_files = third_patry_zip_file.NameToInfo.keys()

            assert "six.py" in all_third_patry_files
            assert "luigi/task.py" in all_third_patry_files

            # check cache
            new_zip_files = build_wheel_zips()
            assert zip_files[0] == new_zip_files[0]
            assert zip_files[1] == new_zip_files[1]
