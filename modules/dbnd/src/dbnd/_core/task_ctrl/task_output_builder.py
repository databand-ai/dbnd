import logging
import os
import random
import re

from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from targets import target
from targets.target_config import TargetConfig
from targets.utils.path import no_trailing_slash


logger = logging.getLogger(__name__)
windows_drive_re = re.compile(r"^[A-Z]\:\\")


def calculate_path(task, name, path_pattern, output_ext="", is_dir=False):
    # do we need to have a path like date=None ?
    # path_vars = [str(v) if v is not None else "" for v in task.get_vars()]
    path_vars = task.get_template_vars()

    sep = "/"

    env = task.ctrl.task_env
    root = no_trailing_slash(str(env.root))
    if windows_drive_re.match(root):
        sep = os.sep
    path = path_pattern.format(
        root=root,
        dbnd_root=str(env.dbnd_root),
        output_name=name,
        output_ext=output_ext or "",
        sep=sep,
        env_label=task.task_env.env_label,
        **path_vars
    )
    if is_dir:
        path += "/"
    return path


class TaskOutputBuilder(TaskSubCtrl):
    """
    we have too many different ways to get "task" requirements
    all this logic should be outside the task object, as this is "meta store"
    """

    def __init__(self, task):
        super(TaskOutputBuilder, self).__init__(task=task)

    def target(self, name, config=None, output_ext=None, output_mode=None):
        task = self.task
        config = config or TargetConfig()
        path_pattern = task._get_task_output_path_format(output_mode)

        path = calculate_path(
            task=task,
            name=name,
            output_ext=output_ext,
            is_dir=config.folder,
            path_pattern=path_pattern,
        )

        return target(path, config=config)

    def get_tmp_output(self, name=None, output_ext=None, config=None):
        name = name or "dbnd-tmp-%09d" % random.randint(0, 999999999)

        return self.target(name="tmp/%s" % name, config=config, output_ext=output_ext)
