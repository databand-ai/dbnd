import os

from distutils.cmd import Command


class BdistFatZip(Command):

    description = "create deps and project distribution files for spark_submit"
    user_options = [
        ("dist-dir=", "d", "Build deps into dir. [default: dist]"),
        ("package-dir=", "p", "The dir of the setup.py file. [default: .]"),
        ("requirements=", "r", "Install from the given requirements file."),
    ]

    def initialize_options(self):
        self.dist_dir = "dist"
        self.package_dir = "."
        self.requirements = None

    def finalize_options(self):
        if self.requirements is not None:
            assert os.path.exists(
                self.requirements
            ), "requirements file '{}' does not exist.".format(self.requirements)

    def run(self):
        from dbnd.tasks.py_distribution.build_distribution import (
            build_fat_requirements_py_zip_file,
        )
        from targets.target_config import folder
        from targets import target

        build_fat_requirements_py_zip_file(
            target(self.dist_dir, config=folder), self.package_dir, self.requirements
        )
