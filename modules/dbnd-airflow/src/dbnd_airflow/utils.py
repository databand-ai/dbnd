import logging
import os.path

from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd._core.utils.project.project_fs import abs_join, relative_path


logger = logging.getLogger(__name__)
_airflow_lib_home_default = relative_path(__file__)


def dbnd_airflow_path(*path):
    return abs_join(_airflow_lib_home_default, *path)


def link_dropin_file(source_path, target_path, unlink_first=True, name="file"):
    sub_dir = os.path.dirname(target_path)

    if unlink_first and os.path.islink(target_path):
        try:
            logger.info("unlinking existing %s at %s", name, target_path)
            os.unlink(target_path)
        except Exception:
            logger.error(
                "failed to unlink %s at %s: %s",
                (name, target_path, format_exception_as_str()),
            )
            return

    if not os.path.exists(target_path):
        try:
            logger.info("Linking %s to %s.", source_path, target_path)
            if not os.path.exists(sub_dir):
                os.makedirs(sub_dir)
            os.symlink(source_path, target_path)
        except Exception:
            logger.error(
                "failed to link %s in the airflow dags_folder: %s"
                % (name, format_exception_as_str())
            )
