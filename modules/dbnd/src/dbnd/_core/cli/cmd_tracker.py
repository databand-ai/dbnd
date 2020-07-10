import logging
import sys
import time

from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.utils.timeout import wait_until
from dbnd._vendor import click
from dbnd._vendor.click import command


logger = logging.getLogger(__name__)


@click.group()
def tracker():
    pass


@tracker.command()
@click.option(
    "--timeout", "-t", type=int, default=120, help="Wait for tracker to be running"
)
def wait(timeout):
    with new_dbnd_context(name="new_context") as dbnd_ctx:
        logger.info("Waiting {} seconds for tracker to become ready:".format(timeout))
        is_ready = wait_until(dbnd_ctx.tracking_store.is_ready, timeout)
        if not is_ready:
            logger.error("Tracker is not ready after {} seconds.".format(timeout))
            sys.exit(1)
        logger.info("Tracker is ready.")
