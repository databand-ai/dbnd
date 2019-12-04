import json
import logging

from collections import Counter
from typing import List

from dbnd import PythonTask, output, parameter, task
from dbnd._core.commands import log_metric
from targets.types import DataList


logger = logging.getLogger(__name__)


class WordCountTask(PythonTask):
    text = parameter.data[List[str]]
    counters = output.data

    #
    factor = parameter.value(
        1, description="just an example parameter, will multiply real count by value"
    )

    def run(self):
        log_metric("input", len(self.text))
        logger.info("Factor: %s", self.factor)
        result = Counter()
        for line in self.text:
            result.update(line.split() * self.factor)

        self.counters.write(json.dumps(result))


@task
def word_count(text, factor=1):
    # type: (DataList[str], int)-> int
    log_metric("input", len(text))
    logger.info("Factor: %s", factor)

    result = Counter()
    for line in text:
        result.update(line.split() * factor)
    return sum(result.values())
