# © Copyright Databand.ai, an IBM Company 2022

# we need to import databand module before airflow, otherwise we will not get airflow_bome

import cProfile
import logging
import os
import random
import time

from datetime import datetime

from _pytest.fixtures import fixture


logger = logging.getLogger(__name__)


@fixture
def cProfile_benchmark():
    perf_stats_dir = "/tmp/benchmark/"

    def _benchmark(fn, *args, **kwargs):
        unique_id = datetime.now().strftime(
            "%Y%m%d-%H%M%S-" + str(random.randint(0, 10000))
        )
        # with perf_trace(os.path.join(perf_stats_dir, "p1_%s.stats" % (unique_id))):
        file = os.path.join(perf_stats_dir, "benchmark_%s.stats" % (unique_id))
        start = time.time()
        pr = cProfile.Profile()
        pr.enable()

        result = fn(*args, **kwargs)
        done = time.time()
        pr.disable()

        file = os.path.abspath(file)
        if not os.path.exists(os.path.dirname(file)):
            os.makedirs(os.path.dirname(file))

        pr.dump_stats(file)
        logging.warning("Performance report saved at %s", file)
        elapsed = done - start
        logger.warning("Time Elapsed for %s: %s", fn, elapsed)
        return result

    return _benchmark
