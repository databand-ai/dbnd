from dbnd import parameter
from dbnd._core.task import Config


class SnowflakeConfig(Config):
    """dbnd-snowflake configuration"""

    _conf__task_family = "snowflake"

    table_preview_rows = parameter(
        default=20,
        description="How many (first) rows to include in Snowflake table preview",
    )[int]

    query_history_result_limit = parameter(
        default=100,
        description="Default value to use when searching for a query resources in QUERY_HISTORY",
    )[int]

    query_history_result_limit_max_value = parameter(
        default=10000,
        description="RESULT_LIMIT param max value accepted by QUERY_HISTORY() function. Hard limit imposed by Snowflake.",
    )[int]

    query_history_end_time_range_start = parameter(
        default=15,
        description="QUERY_HISTORY END_TIME_RANGE_START param. Minutes before now.",
    )[int]

    query_history_end_time_range_end = parameter(
        default=0,
        description="QUERY_HISTORY END_TIME_RANGE_END param. Minutes before now.",
    )[int]
