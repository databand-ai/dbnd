import logging
import sys
import time

from dbnd._core.context.databand_context import new_dbnd_context
from dbnd._core.utils.timeout import timeout
from dbnd._vendor import click


logger = logging.getLogger(__name__)


@click.group(help="Tracker commands to ensure synchronization against webserver")
def tracker():
    pass


@tracker.command(help="Wait for the webserver to initialize")
@click.option(
    "--timeout",
    "-t",
    type=int,
    default=300,
    help="Amount of seconds to wait for webserver until timing out",
)
def wait(timeout):
    with new_dbnd_context(name="new_context") as dbnd_ctx:
        logger.info("Waiting {} seconds for tracker to become ready:".format(timeout))

        is_ready = _wait_for_tracking_store(dbnd_ctx, wait_timeout=timeout)
        if not is_ready:
            logger.error("Tracker is not ready after {} seconds.".format(timeout))
            sys.exit(1)
        logger.info("Tracker is ready.")


def _wait_for_tracking_store(dbnd_ctx, wait_timeout):
    try:
        with timeout(wait_timeout, handler=lambda *args: None):
            start_time = time.time()
            while not dbnd_ctx.tracking_store.is_ready():
                time.sleep(2)  # 2 seconds, 1 doesn't make a difference
                logger.info(
                    "Databand server is still not ready after %s seconds",
                    round(time.time() - start_time),
                )
        return dbnd_ctx.tracking_store.is_ready()
    except TimeoutError:
        pass
    return False
