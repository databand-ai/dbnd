import os
import pprint
import sys

from dbnd._core.tracking.dbnd_spark_init import (
    _is_dbnd_spark_installed,
    try_get_airflow_context_from_spark_conf,
    verify_spark_pre_conditions,
)


pp = pprint.PrettyPrinter(indent=4)


def custom_print(msg):
    print(msg, file=sys.stderr)


def _is_dbnd_airflow_key(k):
    return "dbnd" in k.lower() or "airflow" in k.lower()


def print_dbnd_environment_info():
    custom_print("DBND ENVIRONMENT INFO")
    custom_print("ENVIRON:\n %s" % pp.pformat(os.environ))
    custom_print(
        "DBND AIRFLOW ENVIRON:\n %s"
        % pp.pformat({k: v for k, v in os.environ.items() if _is_dbnd_airflow_key(k)})
    )

    custom_print("SYS PATH:\n %s" % pp.pformat(sys.path))
    custom_print("PYTHON:\n %s" % pp.pformat(sys.executable))


def print_dbnd_spark_info():
    custom_print("IS_DBND_SPARK_INSTALLED:  %s" % _is_dbnd_spark_installed())
    custom_print("VERIFY_SPARK_PRE_CONDITIONS: %s" % verify_spark_pre_conditions())
    print_airflow_context()

    from pyspark import SparkConf

    spark_conf = SparkConf().getAll()
    custom_print("DBND SPARK INFO")
    custom_print("SPARK GET CONF %s" % pp.pformat(spark_conf))
    custom_print(
        "DBND AIRFLOW SPARK GET CONF\n %s"
        % pp.pformat([(k, v) for k, v in spark_conf if _is_dbnd_airflow_key(k)])
    )


def print_airflow_context():
    custom_print("DBND SPARK AIRFLOW CONTEXT")
    try:
        af_context = try_get_airflow_context_from_spark_conf()
        if af_context:
            custom_print("DAG_ID: %s" % af_context.dag_id)
            custom_print("EXECUTION_DATE: %s" % af_context.execution_date)
            custom_print("TASK_ID: %s" % af_context.task_id)
            custom_print("TRY_NUMBER: %s" % af_context.try_number)
            custom_print("AIRFLOW_INSTANCE_UID: %s" % af_context.airflow_instance_uid)
        else:
            custom_print("DBND AIRFLOW CONTEXT is None")
    except Exception as e:
        custom_print("airflow context exception: %s" % pp.pformat(e))


def print_dbnd_debug():
    try:
        print_dbnd_environment_info()
        print_dbnd_spark_info()
    except Exception as e:
        custom_print("debug spark init exception: %s" % pp.pformat(e))


def create_dummy_dataframe(spark):
    try:
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1),
        ]

        schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(data=data, schema=schema)
        return df
    except Exception as e:
        custom_print("create dummy dataframe exception: %s" % pp.pformat(e))


def debug_log_dataset_op_read(df):
    from dbnd import dataset_op_logger

    with dataset_op_logger("dummy_df", "read", with_stats=True) as logger:
        logger.set(data=df)


def debug_log_dataset_op_write(df):
    from dbnd import dataset_op_logger

    with dataset_op_logger("dummy_df", "write", with_stats=True) as logger:
        df.write.format("csv").mode("overwrite").save("dummy_df.csv")
        logger.set(data=df)


def dbnd_log_debug_dataframe(df):
    if df:
        data_schema = {
            "columns": list(df.schema.names),
            "dtypes": {f.name: str(f.dataType) for f in df.schema.fields},
        }

        rows = df.count()
        data_dimensions = (rows, len(df.columns))
        data_schema.update(
            {"size.bytes": int(rows * len(df.columns)), "shape": data_dimensions}
        )
        custom_print("DATAFRAME SCHEMA:\n %s" % pp.pformat(data_schema))
    else:
        custom_print("DATAFRAME is None")


def dbnd_debug_spark_operations(spark):
    try:
        custom_print("dbnd debug spark operations")
        df = create_dummy_dataframe(spark)
        dbnd_log_debug_dataframe(df)
        debug_log_dataset_op_read(df)
        debug_log_dataset_op_write(df)
    except Exception as e:
        custom_print("dbnd debug spark operations exception: %s" % pp.pformat(e))


def dbnd_log_debug_spark():
    # print debug vars before calling dbnd_tracking_start
    print_dbnd_debug()

    # Try to "manually" start dbnd tracking
    try:
        from dbnd import dbnd_tracking_start

        task_run = dbnd_tracking_start()
        if task_run:
            custom_print("DBND_TRACKING_TASK_RUN: %s" % task_run)
            custom_print("DBND_TRACKING_RUN_UID: %s" % task_run.run.run_uid)
    except Exception as e:
        custom_print("dbnd tracking task run exception: %s" % e)

    # test log metric
    from dbnd import log_metric

    log_metric("debug.metric", 10000)

    # print debug vars after calling dbnd_tracking_start
    print_dbnd_debug()
