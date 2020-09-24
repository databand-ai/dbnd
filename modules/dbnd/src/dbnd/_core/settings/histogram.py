from dbnd._core.parameter import PARAMETER_FACTORY as parameter
from dbnd._core.task import Config


class HistogramConfig(Config):
    _conf__task_family = "histogram"

    spark_parquet_cache_dir = parameter(
        default=None,
        description="Enables pre-cache DF using .parquet store at `spark_temp_dir`",
    )[str]

    spark_cache_dataframe = parameter(
        default=False, description="Enables caching of the whole frame"
    )[bool]

    spark_cache_dataframe_column = parameter(
        default=True,
        description="Enables caching of the numerical df during histogram calculation",
    )[str]
