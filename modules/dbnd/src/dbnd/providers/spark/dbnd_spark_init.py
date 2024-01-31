# © Copyright Databand.ai, an IBM Company 2022
import logging
import os
import sys

from dbnd._core.configuration.config_readers import get_environ_config_from_dict
from dbnd._core.configuration.dbnd_config import config as dbnd_config
from dbnd._core.configuration.environ_config import (
    dbnd_log_init_msg,
    spark_tracking_enabled,
)
from dbnd._core.log import dbnd_log_debug, dbnd_log_exception
from dbnd._core.log.dbnd_log import ENV_DBND__VERBOSE, is_verbose, set_verbose
from dbnd._core.utils.seven import import_errors


_IS_SPARK_INSTALLED = None
_SPARK_ENV_FLAG = "SPARK_ENV_LOADED"  # if set, we are in spark

logger = logging.getLogger(__name__)


def _is_dbnd_spark_installed():
    if not spark_tracking_enabled():
        dbnd_log_debug(
            "This is not a spark job: DBND__ENABLE__SPARK_CONTEXT_ENV is not set"
        )
        return False

    if _SPARK_ENV_FLAG not in os.environ:
        dbnd_log_debug(f"This is not a spark job: : {_SPARK_ENV_FLAG} not found in env")
        return False

    if "pyspark" not in sys.modules:
        dbnd_log_debug("This is not a spark job: : 'pyspark' is not found in modules")
        return False

    try:
        from pyspark import SparkContext  # noqa: F401
    except import_errors:
        dbnd_log_debug("This is not a spark job: can not import pyspark module")
        return False

    # all good, we have it
    return True


def verify_spark_pre_conditions():
    global _IS_SPARK_INSTALLED
    if _IS_SPARK_INSTALLED is not None:
        return _IS_SPARK_INSTALLED
    try:
        _IS_SPARK_INSTALLED = _is_dbnd_spark_installed()
    except Exception:
        # safeguard, on any exception
        _IS_SPARK_INSTALLED = False

    return _IS_SPARK_INSTALLED


def has_active_spark_session():
    if not verify_spark_pre_conditions():
        return None

    try:
        from pyspark.sql import SparkSession

        logger.debug(
            "Spark session SparkSession._instantiatedSession %s",
            SparkSession._instantiatedSession,
        )
        return SparkSession._instantiatedSession is not None
    except import_errors:
        return False


def _safe_get_active_spark_context():
    if not verify_spark_pre_conditions():
        return None
    try:
        from pyspark import SparkContext

        if SparkContext._jvm is not None:
            return SparkContext._active_spark_context
        else:
            # spark context is not initialized at this step
            dbnd_log_debug("SparkContext._jvm is not set")
    except Exception as ex:
        dbnd_log_exception("Failed to get SparkContext: %s", ex)


def _safe_get_jvm_view():
    try:
        spark_context = _safe_get_active_spark_context()
        if spark_context is not None:
            return spark_context._jvm
    except Exception as ex:
        logger.info("Failed to get jvm from SparkContext : %s", ex)


def _safe_get_spark_conf():
    try:
        spark_context = _safe_get_active_spark_context()
        if spark_context is not None:
            return spark_context.getConf()
    except Exception as ex:
        logger.info("Failed to get SparkConf from SparkContext : %s", ex)


def get_value_from_spark_env(key):
    try:
        conf = _safe_get_spark_conf()
        if not conf:
            return None

        value = conf.get("spark.env." + key)
        if value:
            return value
    except Exception:
        return None


def load_spark_env():
    spark_conf = _safe_get_spark_conf()
    if not spark_conf:
        return

    dbnd_log_init_msg("loading DBND configuration using spark conf")
    spark_conf = dict(spark_conf.getAll())
    spark_conf = {key.lstrip("spark.env."): value for key, value in spark_conf.items()}

    if ENV_DBND__VERBOSE in spark_conf:
        from dbnd._core.utils.basics.helpers import parse_bool

        if parse_bool(spark_conf.get(ENV_DBND__VERBOSE)):
            set_verbose()

    dbnd_environ = get_environ_config_from_dict(spark_conf, "environ")
    dbnd_config.set_values(dbnd_environ, "spark.env")

    dbnd_log_init_msg("DBND configuration from spark conf: %s" % dbnd_environ)

    return dbnd_environ


def try_load_spark_env():
    try:
        if not verify_spark_pre_conditions():
            return None

        dbnd_log_init_msg("Spark pre conditions are verified")
    except Exception:
        dbnd_log_init_msg("Failed to verify spark pre-conditions")
        return None

    try:
        load_spark_env()
        dbnd_log_init_msg("Spark env is loaded")
    except Exception:
        if is_verbose():
            dbnd_log_exception("Failed to load spark env")
        return None
