# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.task import Config


class HistogramConfig(Config):
    _conf__task_family = "histogram"

    spark_parquet_cache_dir = parameter(
        default=None,
        description="Enables pre-caching of DataFrames using .parquet store at `spark_temp_dir`",
    )[str]

    spark_cache_dataframe = parameter(
        default=False,
        description="Determine whether to cache the whole DataFrame or not.",
    )[bool]

    spark_cache_dataframe_column = parameter(
        default=True,
        description="Enable caching of the numerical DataFrame during histogram calculation.",
    )[str]
