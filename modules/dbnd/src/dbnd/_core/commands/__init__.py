from dbnd._core.commands.metrics import (
    log_artifact,
    log_dataframe,
    log_duration,
    log_metric,
    log_metrics,
)
from dbnd._core.plugin.dbnd_plugins import assert_plugin_enabled


def get_spark_session():
    assert_plugin_enabled("dbnd-spark")
    import dbnd_spark

    return dbnd_spark.get_spark_session()
