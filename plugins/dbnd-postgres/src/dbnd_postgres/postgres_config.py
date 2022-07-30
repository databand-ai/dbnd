# © Copyright Databand.ai, an IBM Company 2022

from dbnd import Config, parameter


class PostgresConfig(Config):
    _conf__task_family = "postgres"
    auto_log_pg_histograms = parameter(
        description="Automatically log all postgres table histograms", default=True
    )[bool]
