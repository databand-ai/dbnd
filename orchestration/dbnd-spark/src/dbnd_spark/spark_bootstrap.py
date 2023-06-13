# © Copyright Databand.ai, an IBM Company 2022

# do not import PySpark until we run run dbnd_spark_bootstrap
import collections
import logging
import sys
import traceback


logger = logging.getLogger(__name__)
_DBND_REGISTER_SPARK_TYPES = None


def workaround_spark_namedtuple_serialization():
    global _DBND_REGISTER_SPARK_TYPES
    if _DBND_REGISTER_SPARK_TYPES:
        return
    # don't run it twice or in recursion
    _DBND_REGISTER_SPARK_TYPES = True

    _workaround_spark_namedtuple_serialization()
    # we register spark types only if we have spark installed
    return


def _workaround_spark_namedtuple_serialization():
    """
    We are checking that we are running in DatProcessor for scheduler,
    DagProcessor will send DAG results back to scheduler via Pipe
    Serialization of DAG with namedtuple will fail because of pyspark patch on namedtuple
    We are going to disable it by "faking" that patch is applied already.
    The moment pyspark will run for real in `airflow run` operator -> this patch is not going to be applied
    and spark will work as usual
    :return:
    """
    if "scheduler" not in sys.argv:
        return

    running_at_scheduler = False
    for filename, line_number, name, line in traceback.extract_stack():
        if line and line.strip() == "self.processor_agent.start()":
            running_at_scheduler = True

    if not running_at_scheduler:
        return

    logging.debug("Preventing pyspark from namedtuple patch")
    setattr(collections.namedtuple, "__hijack", True)
