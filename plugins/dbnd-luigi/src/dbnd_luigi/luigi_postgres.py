# © Copyright Databand.ai, an IBM Company 2022

import luigi

from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled


def handle_postgres_histogram_logging(luigi_task):
    # type: (luigi.Task) -> None
    from dbnd_postgres.postgres_config import PostgresConfig

    conf = PostgresConfig()
    if not conf.auto_log_pg_histograms:
        return
    postgres_target = luigi_task.output()
    from dbnd._core.tracking.metrics import log_pg_table

    log_pg_table(
        table_name=postgres_target.table,
        connection_string="postgres://{}:{}@{}:{}/{}".format(
            postgres_target.user,
            postgres_target.password,
            postgres_target.host,
            postgres_target.port,
            postgres_target.database,
        ),
        with_histograms=True,
    )


def should_log_pg_histogram(luigi_task):
    # type: (luigi.Task) -> bool
    if not is_plugin_enabled("dbnd-postgres", module_import="dbnd_postgres"):
        return False
    try:
        from luigi.contrib.postgres import PostgresQuery
    except ImportError:
        return False
    return isinstance(luigi_task, PostgresQuery)
