import logging

from datetime import timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago
from pyspark.sql import DataFrame

from dbnd import parameter, relative_path, task
from dbnd_spark.spark import PySparkTask, spark_task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    #  "dbnd_config": {"databand": {"env": "gcp"}},
}


@task
def t_A(p_str="check", p_int=2):
    # type: (str,int) -> str
    logging.info("I am running")
    return p_str * p_int


@spark_task
def word_count_inline(text: DataFrame) -> DataFrame:
    from operator import add

    from dbnd_spark.spark import get_spark_session

    lines = text.rdd.map(lambda r: r[0])
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    return get_spark_session().createDataFrame(counts)


class WordCountPySparkTask(PySparkTask):
    text = parameter.data
    counters = parameter.output

    python_script = relative_path(__file__, "spark_scripts/word_count.py")

    def application_args(self):
        return [self.text, self.counters]


with DAG(dag_id="dbnd_dag_with_spark", default_args=default_args) as dag_spark:
    # noinspection PyTypeChecker
    spark_task = WordCountPySparkTask(text="s3://dbnd/README.md")
    spark_op = spark_task.op
    # spark_result = word_count_inline("/tmp/sample.txt")
    # spark_op = spark_result.op

if __name__ == "__main__":
    ti = TaskInstance(spark_op, days_ago(0))
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    # # #
    #
    # dag_spark.clear()
    # dag_spark.run(start_date=days_ago(0), end_date=days_ago(0))
