import logging

from collections import Counter

from dbnd import log_metric, task
from targets.types import DataList


logger = logging.getLogger(__name__)


@task
def word_count(text, factor=1):
    # type: (DataList[str], int)-> int
    log_metric("input", len(text))
    logger.info("Factor: %s", factor)

    result = Counter()
    for line in text:
        result.update(line.split() * factor)
    return sum(result.values())
