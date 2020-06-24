import os
import zipfile

from dbnd import dbnd_config
from dbnd.tasks.py_distribution.fat_wheel_builder import build_fat_wheel
from dbnd_test_scenarios.scenarios_repo import test_scenario_path


class TestBuildFatWheel(object):
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
