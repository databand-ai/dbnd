# © Copyright Databand.ai, an IBM Company 2022

import os
import zipfile

from dbnd import dbnd_config
from dbnd_run.tasks.py_distribution.fat_wheel_tasks import ProjectWheelFile
from dbnd_test_scenarios.scenarios_repo import test_scenario_path


class TestBuildFatWheel(object):
    def test_fat_wheel_task(self):
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
