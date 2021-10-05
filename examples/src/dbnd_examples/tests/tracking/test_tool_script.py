import logging
import sys

from dbnd import output, relative_path, task
from dbnd.tasks.basics.shell import bash_cmd
from targets.types import PathStr


logger = logging.getLogger(__name__)


def cli_scripts(*path):
    return relative_path(__file__, *path)


@task(my_cli_output=output[PathStr])
def my_cli(parameter, my_cli_input, my_cli_output):
    # type: (int, PathStr, PathStr) -> None
    bash_cmd(
        args=[
            sys.executable,
            cli_scripts("../../tracking/tracking_script.py"),
            my_cli_output,
            parameter,
            my_cli_input,
        ],
        check_retcode=1,
    )
