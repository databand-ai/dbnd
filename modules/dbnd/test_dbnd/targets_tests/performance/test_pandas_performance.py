# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging
import os
import time

import pandas as pd
import pytest

from dbnd._core.utils.project.project_fs import project_path
from targets import target
from targets.target_config import file


logger = logging.getLogger(__name__)


def format_readable_size(num):
    byte_size = 1024.0
    counter = 0
    while abs(num) >= byte_size:
        counter += 1
        num /= byte_size

    suffix_list = ["", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"]
    if counter >= len(suffix_list):
        return f"{num:.1f}YiB"

    return f"{num:.1f}{suffix_list[counter]}"


def sample_path(*path):
    return project_path("tests/dbnd_airflow/targets_tests/performance/data", *path)


def sample_generated(replication, ext):
    return sample_path("sample_%s.%s" % (replication, ext))


def sample_perf(replication, ext):
    return sample_path("sample_perf_%s%s" % (replication, ext))


@pytest.mark.skip("performance tests")
class TestPandasPerformance(object):
    def test_create_dataset(self):
        sample = target(sample_path("sample_data.csv")).as_pandas.read_csv()

        for x in range(2, 5, 1):
            replication = pow(100, x)
            logger.info("Sampling %s", replication)
            sample = pd.concat([sample] * 100)

            t = target(sample_generated(replication, file.csv))
            t.as_pandas.to_csv(sample)
        logger.info("Sample %s -> %s ", replication, os.stat(t.path).st_size)

    def test_performance(self):
        files = [pow(100, x) for x in range(1, 3)]
        # files = [100000000]
        all_status = []
        for replication in files:
            logger.info("Sampling %s", replication)

            t = target(sample_generated(replication, file.csv))
            actual_data = t.as_pandas.read_csv()
            for target_config in [file.csv, file.hdf5, file.parquet, file.csv.gzip]:
                status = self._run_benchmark(actual_data, replication, target_config)
                all_status.append(status)
        time.sleep(1)
        logger.info("\n\nReport: \n %s\n\n", "\n".join(all_status))

    def _run_benchmark(self, actual_data, replication, config):
        ext = config.get_ext()
        t = target(sample_perf(replication, ext), config=config)
        actual_data.to_target(t)
        logger.info(
            "{replication} {config} - {size}   ".format(
                replication=replication, config=config, size=os.stat(t.path).st_size
            )
        )
        # we want to read target without cache!
        t = target(sample_perf(replication, ext), config=config)
        actual_t_df = t.read_df()
        logger.info("Memory %s", actual_t_df.memory_usage())
        memory = ""
        status = "{replication} {config}  - {size} on disk, {memory} in memory".format(
            replication=replication,
            config=config,
            size=format_readable_size(os.stat(t.path).st_size),
            memory=memory,
        )
        logger.info(status)
        return status
