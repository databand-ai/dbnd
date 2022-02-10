import os

import pytest

from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.utils.project.project_fs import databand_lib_path


class TestUserCodeDetector:
    code_dir = "users/test_user"
    databand_path = databand_lib_path()

    @pytest.fixture
    def user_code_detector(self):
        system_code_dirs = [databand_lib_path(), databand_lib_path("../targets")]
        return UserCodeDetector(
            code_dir=self.code_dir, system_code_dirs=system_code_dirs
        )

    @pytest.mark.parametrize(
        "file_name,expected_result",
        [
            (f"test/site-packages/my_python_package/main.py", False),
            (f"test/dist-packages/my_python_package/main.py", False),
            (f"{databand_path}/module/databand_module", False),
            (f"/etc/hosts", False),
            (f"{code_dir}/dbnd_examples/dbnd_sanity_check", True),
            ("dbnd_examples/dbnd_sanity_check", True),
            (f"{code_dir}/my_scripy.py", True),
        ],
    )
    def test_is_user_file(self, user_code_detector, file_name, expected_result):
        is_user_file = user_code_detector.is_user_file(file_name)
        assert is_user_file == expected_result
