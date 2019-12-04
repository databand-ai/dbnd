import os
import re

from dbnd import parameter
from dbnd._core.task.config import Config


class SparkLogParserConfig(Config):
    """(Advanced) Apache Spark log parser"""

    _conf__task_family = "spark_log_parser"

    error_regex_pattern = parameter.value(
        default="([A-Z][a-z]+)+Error| Error | Exception ",
        description="regular expression to find errors in spark logs.",
    )

    lines_to_show = parameter.value(
        default=4, description="log lines to show for each error snippet"
    )

    snippets_to_show = parameter.value(
        default=3, description="error snippets to show in error message"
    )


def parse_spark_log(lines):
    spark_log_config = SparkLogParserConfig()
    error_pattern = re.compile(spark_log_config.error_regex_pattern)
    errors_snippet = []
    index = 0
    while index < len(lines):
        if error_pattern.search(lines[index]):
            error = "spark driver log:%i %s\n" % (
                index,
                os.linesep.join(lines[index : index + spark_log_config.lines_to_show]),
            )
            errors_snippet.append(error)
            index = index + spark_log_config.lines_to_show
        else:
            index = index + 1
    return errors_snippet[: spark_log_config.snippets_to_show]
