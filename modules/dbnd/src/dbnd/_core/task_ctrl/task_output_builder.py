import logging
import os
import re

from targets.utils.path import no_trailing_slash


logger = logging.getLogger(__name__)
windows_drive_re = re.compile(r"^[A-Z]\:\\")


def calculate_path(task, name, path_pattern, output_ext="", is_dir=False):
    # do we need to have a path like date=None ?
    # path_vars = [str(v) if v is not None else "" for v in task.get_vars()]
    path_vars = task.get_template_vars()

    sep = "/"

    root = no_trailing_slash(str(task.get_root()))
    if windows_drive_re.match(root):
        sep = os.sep
    task_env = task.task_env
    path = path_pattern.format(
        root=root,
        dbnd_root=str(task_env.dbnd_root),
        output_name=name,
        output_ext=output_ext or "",
        sep=sep,
        env_label=task_env.env_label,
        **path_vars
    )
    if is_dir:
        path += "/"
    return path
