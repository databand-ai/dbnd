import logging
import os.path
import shlex
import sys
import typing

from dbnd import dbnd_run_cmd
from dbnd._core.current import try_get_databand_context
from dbnd._core.utils.platform import windows_compatible_mode


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task.task import Task

logger = logging.getLogger(__name__)


def _get_run_name():
    # implement code that can find name from PyTest context

    f = sys._getframe().f_back
    while f.f_code.co_filename == __file__:
        f = f.f_back
    return os.path.basename(f.f_code.co_filename).replace(".py", "")


def run_cmd_locally(args):
    """
    deprecated, please use dbnd_run_cmd directly
    """
    return dbnd_run_cmd(args=args)


def run_cmd_locally_split(space_seperated_args):
    """ Helper for running tests testing more of the stack, the command
    line parsing and task from name intstantiation parts in particular. """
    return dbnd_run_cmd(
        shlex.split(space_seperated_args, posix=not windows_compatible_mode)
    )


def run_task(task):
    # type: (Task)-> DatabandRun
    # this code should be executed under context!
    return task.dbnd_run()


def set_current(name, description):
    try_get_databand_context().set_current(name, description=description)
