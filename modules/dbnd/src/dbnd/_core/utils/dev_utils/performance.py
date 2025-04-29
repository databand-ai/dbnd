# © Copyright Databand.ai, an IBM Company 2022

import cProfile
import logging
import os

from decorator import contextmanager


@contextmanager
def perf_trace(file):
    pr = cProfile.Profile()
    pr.enable()
    yield pr
    pr.disable()

    file = os.path.abspath(file)
    os.makedirs(os.path.dirname(file), exist_ok=True)

    pr.dump_stats(file)
    logging.warning("Performance report saved at %s", file)
