---
"title": "Tracking PySpark"
---
If you are running jobs in PySpark, Databand can provide visibility into your code errors, metrics, and logging information, in the context of your broader pipelines or orchestration system.

Like for Python, you can use Databand [decorators and the logging API ](doc:python) for Spark jobs in a similar way. Here is an example of a PySpark function with Databand decoration and metrics' logging:

``` python
import sys
from operator import add
from pyspark.sql import SparkSession
import logging
from dbnd import log_metric, task

logger = logging.getLogger(__name__)

@task
def calculate_counts(input_file, output_file):
    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    counts.saveAsTextFile(output_file)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
    log_metric("counts", len(output))
    logger.info(
        "Log message from EMR cluster"
    )
    spark.sparkContext.stop()


if __name__ == "__main__":
    calculate_counts(sys.argv[1], sys.argv[2])
```

In this code example, there are a number of artifacts that will be reported to the DBND tracking system.
 
The first one is the output of the following Python snippet: 

```python 
for (word, count) in output: print("%s: %i" % (word, count))
```

The second is Databand's `log_metric` API, which reports a count. When you use Python logging facilities, for example - `logger.info` (line below the `log_metric` API in the code), all logs are also reported.

Databand will correlate the tracking metrics from the Spark job with the associated pipeline from your orchestration system (for example, an Airflow DAG) based on the user design.

You can run this script in the following way:
``` bash 
# enable explicit tracking for @task code
export DBND__TRACKING=True
export DBND__ENABLE__SPARK_CONTEXT_ENV=True

# make sure you have your login information, this way or another
export DBND__CORE__DATABAND_URL=...
export DBND__CORE__DATABAND_ACCESS_TOKEN=...

spark-submit --conf "spark.env.DBND__RUN__NAME=my_run_name"  my_pyspark_script.py <path/to/input_file> <path/to/output_file>
```

## Tracking Dataframes
You can use the dataset logging API to track Spark DataFrame as described in [Tracking Datasets](doc:tracking-python-datasets).
 

## Integrating with Databand Listeners.
Your PySpark script can benefit from automatic tracking of Spark Metrics and IO information. Please see detailed information at [JVM SDK Configuration](doc:jvm-sdk-configuration).

``` bash
# Please export all relevant environment variables here.

spark-submit --driver-java-options "-javaagent:/PATH_TO_AGENT" --conf "spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener"  \
             my_pyspark_script.py <path/to/input_file> <path/to/output_file>
```


## Integrating with PyDeequ for Data Quality Metrics
Databand can collect and send [PyDeequ](https://github.com/awslabs/python-deequ/) metrics. 
 
Please follow up on this installation guide https://pydeequ.readthedocs.io/en/latest/README.html#installation
Make sure that DBND JVM client is part of your spark application including `ai.databand:dbnd-api-deequ` package [Installing JVM DBND Library and Agent](doc:installing-jvm-dbnd) 

### DBND Python Deequ Metrics Repository
In order to connect Databand to Deequ use `DbndDeequMetricsRepository` as in the following example. See more details at [Deequ Repository Documentation]( https://pydeequ.readthedocs.io/en/latest/README.html#repository):

``` python
result_key = ResultKey(spark, ResultKey.current_milli_time(), {"name": "words"})
analysis_runner = AnalysisRunner(spark).onData(lines)

# implement your Deequ Validations , for example : 
# .addAnalyzer( ApproxCountDistinct("value") )

analysis_runner.useRepository(DbndMetricsRepository(spark)).saveOrAppendResult(result_key).run()
```

> ℹ️ If you are running Scala or Java Spark
> Please refer to our page for [Tracking Spark (Scala/Java)](doc:jvm).
[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>"
}
[/block]