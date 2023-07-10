# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import pipeline, task


logger = logging.getLogger(__name__)


@task
def calculate_alpha(base: int):
    calculated_alpha = base + 1
    logging.info("Alpha=%s", calculated_alpha)
    return calculated_alpha


@task
def calculate_ratio(base: int):
    calculated_ratio = base + 1
    logging.info("Ratio=%s", calculated_ratio)
    return calculated_ratio


@pipeline
def calculate_values_pipeline(base=1):
    alpha = calculate_alpha(base)
    ratio = calculate_ratio(base)
    return alpha, ratio


#####
# Now we can create a task that calculates base and a pipeline that wraps everything together


@task
def calculate_base(base: int = 5):
    calculated_base = base + 1
    logging.info("Base value=%s", calculated_base)
    return calculated_base


@pipeline
def calculate_values_with_base_pipeline():
    base = calculate_base()
    alpha, ratio = calculate_values_pipeline(base=base)
    return base, alpha, ratio


if __name__ == "__main__":
    result = calculate_values_with_base_pipeline.dbnd_run()
    logger.info("Result: %s", result)
