# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging
import sys


logger = logging.getLogger(__name__)

try:
    import newrelic.agent

    newrelic_available = True
except ImportError:
    newrelic_available = False


@contextlib.contextmanager
def transaction_scope(name: str):
    if not newrelic_available:
        yield
        return

    try:
        background_task = newrelic.agent.BackgroundTask(
            application=newrelic.agent.application(),
            name=name,
            group=sys.argv[0].rsplit("/", 1)[-1],
        )
    except Exception:
        logger.warning("Error while creating newrelic background_task", exc_info=True)
        background_task = None

    if not background_task:
        yield
        return

    with background_task:
        yield
