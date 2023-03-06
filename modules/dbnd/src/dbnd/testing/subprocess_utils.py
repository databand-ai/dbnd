# © Copyright Databand.ai, an IBM Company 2022

import logging
import subprocess


logger = logging.getLogger(__name__)


def _wrap_output(cmd, output):
    return (
        f" **********  Subprocess Output '{cmd}'  \n"
        f"{output}\n"
        f" **********  Subprocess End of Output '{cmd}'  \n"
    )


def run_subproces_check_output(cmd, check=True):
    logger.info("Running cmd: %s", cmd)
    cmd_str = cmd if isinstance(cmd, str) else subprocess.list2cmdline(cmd)
    logger.info("Running cmd: %s", cmd_str)
    try:
        result = subprocess.run(
            cmd_str,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,  # nosec B602 (Ignore this for bandit as this is used only for test scenarios)
            check=check,
        )

        output = result.stdout.decode("utf-8").strip()
        logger.info(f"Success run {cmd_str}")
        logger.info(f"\n\n\n{_wrap_output(cmd_str, output)}")

    except subprocess.CalledProcessError as ex:
        logger.error(
            "Failed to run %s\n%s",
            cmd,
            _wrap_output(cmd_str, ex.output.decode("utf-8", errors="ignore")),
        )
        raise ex
    return output
