from dbnd import parameter, project_path
from dbnd._core.task.config import Config


class FatWheelConfig(Config):
    _conf__task_family = "bdist_zip"
    _conf__help_description = "Configuration for building fat wheel file that contains project and third party requirements"
    package_dir = parameter.c(
        default=project_path(),
        description="Full path to the directory of the package that contains setup.py",
    )[str]
    requirements_file = parameter.c(
        description="Full path to the requirements file (if any)", default=None
    )[str]
