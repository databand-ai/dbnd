import logging

from collections import Counter

from dbnd import log_metric, pipeline, task
from targets.types import DataList


logger = logging.getLogger(__name__)

"""
# Open issues
## Musts:
+ 1. currently no tracking
+ 5. log metric from inline_task not working - no task_id in current databand_run
+ 9. dependenices to current task
+ 7. invoke inline pipeline not working (inline, as regular task)

---
4. data management/signature
  a. should signature always be unique? - invoke same function multiple times
    add parent to signatrue
  b. should inputs be converted to targets? (opt)
    + pass through targetst (dict obj_id -> target)
    - convert data inputs to targets
    -- Issues
    child task not necessary has type definition (DataList[str])
     => passing targets may not work (or we can infer "parent" type for child parameter?)
     => not necessary we'll know to properly serialize any input object (pickle?)

     ui - show target name or value?
     dependency to "file__task"?

     only for tracking?

---
+8. pipeline creates databand_run() while task - just inline

- 3. create databand_run or update existing one?

"""


@task
def word_count(text, factor=1):
    # type: (DataList[str], int)-> int
    log_metric("input", len(text))
    logger.info("Factor: %s", factor)

    result = Counter()
    for line in text:
        result.update(line.split() * factor)
    return sum(result.values())


@pipeline
def word_count_p(text, dist=0):
    # type: (DataList[str])-> int
    return word_count(text)


@task
def wrap_word_count(text, factor=1):
    # type: (DataList[str], int)-> int
    result = word_count_p(text, dist=1)
    return result


# dbnd run -m dbnd_examples.examples.inline_tasks inline_word_count --task-version now --inline-word-count-text=`pwd`/LICENSE
@pipeline
def inline_word_count(text):
    # type: (DataList[str])-> int
    return wrap_word_count(text)


@task
def compare(a, b):
    return a == b


@pipeline
def uber_word_count(text):
    val1 = inline_word_count(text)
    val2 = word_count_p(text)
    return compare(val1, val2)


class Stepper:
    def __init__(self):
        self.value = 0

    def step(self):
        self.value += 1
        return self.value


stepper = Stepper()


@task
def a():
    log_metric("step", stepper.step())
    return "a"


@task
def b():
    log_metric("step", stepper.step())
    return "b({})".format(a())


@task
def c():
    log_metric("step", stepper.step())
    return "c({}, {})".format(b(), a())


# dbnd run -m dbnd_examples.examples.inline_tasks main_main --task-version now
@task
def main_main():
    log_metric("step", stepper.step())
    return "main({})".format(c())
