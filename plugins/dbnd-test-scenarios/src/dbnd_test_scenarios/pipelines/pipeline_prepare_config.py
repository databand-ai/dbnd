import logging

from dbnd import log_metric, pipeline, task
from targets.types import PathStr


logger = logging.getLogger(__name__)


@task
def create_model(y_input="y"):
    logger.info("Running %s -> operation_y", y_input)
    log_metric("a", 1)
    return "{} -> operation_y".format(y_input)


@task
def prepare_cfg(base_cfg, model):
    # type:(PathStr,PathStr) -> object
    return {"a": model}


@task
def run_cfg(cfg, coeff=2):
    # type:(PathStr, int) -> str
    logger.info("Running  %s -> operation_z", cfg)
    return "cfg({})".format(cfg)


@pipeline
def pipe_operations(base_cfg):
    # type: (PathStr)->str
    model = create_model()
    cfg = prepare_cfg(base_cfg=base_cfg, model=model)
    return run_cfg(cfg=cfg)
