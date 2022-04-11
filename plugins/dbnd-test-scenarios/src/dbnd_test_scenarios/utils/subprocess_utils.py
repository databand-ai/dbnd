import logging
import subprocess


logger = logging.getLogger(__name__)


def _wrap_output(cmd, output):
    return (
        f" **********-= Output {cmd} =-********\n"
        f"{output}\n"
        f" **********-= End of Output {cmd} =-*************\n"
    )


def run_subproces_check_output(cmd):
    logger.info("Running cmd: %s", cmd)
    try:
        output = (
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            .decode("utf-8")
            .strip()
        )
        logger.info(f"Success run {cmd} :\n{_wrap_output(cmd, output)}")

    except subprocess.CalledProcessError as ex:
        logger.error(
            "Failed to run %s\n%s",
            cmd,
            _wrap_output(cmd, ex.output.decode("utf-8", errors="ignore")),
        )
        raise ex
    return output
