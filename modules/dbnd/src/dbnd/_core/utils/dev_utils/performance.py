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
    if not os.path.exists(os.path.dirname(file)):
        os.makedirs(os.path.dirname(file))

    pr.dump_stats(file)
    logging.warning("Performance report saved at %s", file)
