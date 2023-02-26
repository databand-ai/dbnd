# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import warnings

from dbnd._core.configuration.environ_config import should_fix_pyspark_imports
from dbnd._core.utils.basics.signal_utils import register_graceful_sigterm
from dbnd._core.utils.platform.osx_compatible.requests_in_forked_process import (
    enable_osx_forked_request_calls,
)


def _surpress_loggers():
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.WARN)


def _suppress_warnings():
    warnings.simplefilter("ignore", FutureWarning)


def fix_pyspark_imports():
    import sys

    pyspark_libs, regular_libs = [], []
    for p in sys.path:
        if "spark-core" in p or "pyspark.zip" in p:
            pyspark_libs.append(p)
        else:
            regular_libs.append(p)
    regular_libs.extend(pyspark_libs)
    sys.path = regular_libs


def dbnd_bootstrap_orchestration(dbnd_entrypoint=False):
    _surpress_loggers()
    _suppress_warnings()
    enable_osx_forked_request_calls()
    register_graceful_sigterm()

    if should_fix_pyspark_imports():
        fix_pyspark_imports()

    from dbnd import dbnd_config

    # if we are running from "dbnd" entrypoint, we probably do not need to load Scheduled DAG
    # this will prevent from every airflow command to access dbnd web api
    if dbnd_config.getboolean("airflow", "auto_disable_scheduled_dags_load"):
        os.environ["DBND_DISABLE_SCHEDULED_DAGS_LOAD"] = "True"
