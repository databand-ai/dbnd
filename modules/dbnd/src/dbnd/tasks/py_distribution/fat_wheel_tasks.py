# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import Task, output, parameter, project_path
from dbnd._core.current import try_get_databand_context
from dbnd.tasks.py_distribution.build_distribution import (
    _create_temp_working_dir,
    generate_project_whl,
    generate_third_party_deps,
    zip_dir,
)
from targets import FileTarget
from targets.types import PathStr


logger = logging.getLogger(__name__)


class ProjectWheelFile(Task):
    task_is_system = True
    package_dir = parameter(
        default=project_path(),
        description="Full path to the directory of the package that contains setup.py",
    )[PathStr]

    requirements_file = parameter(
        description="Full path to the requirements file (if any)", default=None
    )[PathStr]

    wheel_file = output.with_format("zip")

    def run(self):
        local_wheel_file_zip = self.current_task_run.local_task_run_root.partition(
            name="project_wheel_file.zip"
        )  # type: FileTarget
        local_wheel_file_zip.mkdir_parent()
        # we need a temp folder to create our wheel file,
        # pip build doesn't take "output" parameter
        with _create_temp_working_dir() as tmp_build_dir:
            logger.info(
                "Started building bdist_zip file %s at %s, package_dir=%s",
                self.wheel_file,
                tmp_build_dir,
                self.package_dir,
            )

            # generate main file, we can find a package version and name from it if required
            generate_project_whl(self.package_dir, tmp_build_dir)
            if self.requirements_file is not None:
                generate_third_party_deps(self.requirements_file, tmp_build_dir)

            # zip everything into one file before we copy
            zip_dir(local_wheel_file_zip, tmp_build_dir)

            self.wheel_file.copy_from_local(local_wheel_file_zip)
            logger.info(
                "Successfully published project wheel zip file at %s", self.wheel_file
            )

    @classmethod
    def build_project_wheel_file_task(cls):
        # Use current_context_uid to make sure this task is going to be run only once per pipeline
        # Constant task_target_date so the signature won't change if user changes task_target_date parameter.
        fat_wheel_task = cls(
            # we need it to run every time we "rerun" the pipeline
            task_version=try_get_databand_context().current_context_uid,
            # we don't want to inherit from parent task, as they might have different target_dates
            task_target_date="today",
        )
        return fat_wheel_task
