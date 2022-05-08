---
"title": "Spark Integration"
---
[Apache Spark](https://spark.apache.org/) is a unified analytics engine for large-scale data processing. In this section, we show how to configure Spark integration and implement Spark tasks with DBND.



>ℹ️ Please make sure you have configured DBND to work with your spark cluster
> [Spark Configuration](doc:spark-configuration)

## Creating Spark Tasks

DBND supports the following ways to write spark logic:
 * Inline Spark tasks -  a function that takes Spark DataFrame(s) as an input and returns a Spark DataFrame(s) as an output
 * PySpark tasks - a task that takes any arbitrary PySpark script and wraps it into a DBND pipeline
 * JVM Task -  a task that takes any arbitrary Java/Scala script and wraps it into a DBND pipeline.


## Inline Spark Task
The simplest way to write a Spark task with DBND is to:
* Write a function that gets a Spark DataFrame, transforms it using Spark APIs and returns a transformed Spark DataFrame
* Annotate this function using DBND's `@spark_task` decorator

The example below shows a word count Spark function wrapped with a DBND decorator.

```python
import pyspark.sql as spark
from dbnd_spark.spark import spark_task

@spark_task
def prepare_data_spark(text: spark.DataFrame)->spark.DataFrame:
    from operator import add

    lines = text.rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )

    return  counts
```

This task will run on a Spark cluster (the local Spark installation by default) and return a DataFrame with the word index.

Inline Spark provides access to Spark sessions using the  `get_spark_session()` method. Spark DataFrames are supported as input parameters and as function output.

To run this task on DBND:

```shell
dbnd run prepare_data_spark --set text=<some file>
```

## PySpark Task
Another common option is to run an existing PySpark script using DBND. To do this, specify the name of the script and define input/output parameters:

```python
from dbnd import output, parameter
from dbnd_spark import PySparkTask
class PrepareDataPySparkTask(PySparkTask):
    text = parameter.data
    counters = output

    python_script = spark_folder("pyspark/word_count.py")

    def application_args(self):
        return [self.text, self.counters]
```

In the example above, you can see the `python_script` points to the Python Spark code, while `application_args` is a list of command-line arguments passed to the script.

## JVM Spark Task
To run a Spark job implemented with Java/Scala, provide the name of the JAR and the name of the main class to be used by `spark_submit`:

```python
from dbnd import output, parameter
from dbnd_spark import SparkTask, SparkConfig

class PrepareDataSparkTask(SparkTask):
    text = parameter.data
    counters = output

    main_class = "ai.databand.examples.WordCount"
    defaults = {SparkConfig.main_jar: "${DBND_HOME}/databand_examples/tool_spark/spark_jvm/target/ai.databand.examples-1.0-SNAPSHOT.jar"}

    def application_args(self):
        return [self.text, self.counters]
```

In the example above, you can see `main_class` points to the main Java class, while `application_args` is a list of command-line arguments passed to the class.

## Pipelines with Spark Tasks
 Spark tasks are not different from the regular task when you use them at @[pipeline task](doc:tasks-wiring-at-pipeline)