---
"title": "Reusing Spark Context in the Same Process"
---
You can debug inline Spark tasks locally by providing an existing context to it and configuring DBND to run in this context.

**Enable Inplace Spark Context** 

Using configuration file:  
```
[spark_local]
enable_spark_context_inplace = True
```
or set `DBND__SPARK_LOCAL__ENABLE_SPARK_CONTEXT_INPLACE` to `True`
or use context `with config({SparkLocalEngineConfig.enable_spark_context_inplace: True})`
** Running Spark Job with Debugger **

``` python
@spark_task(result=output[spark.DataFrame])
def word_count_inline(data=parameter.csv[spark.DataFrame]):
    # spark business logic goes here
    # set a breakpoint here with a debuger of your choice

# invoke spark task  this way
if __name__ == "__main__":
   # create spark context and run spark task inside this context
    with spark.SparkSession.builder.getOrCreate() as sc:
        word_count_inline.dbnd_run(text=__file__)
```

** Testing Spark Job in the same process **
This technique can help you debug your script as well as improve test runtime duration

``` python
    def test_spark_inline_same_context(self):
        from pyspark.sql import SparkSession
        from dbnd_examples.orchestration.dbnd_spark.word_count import word_count_inline

        with SparkSession.builder.getOrCreate() as sc:
            with config({SparkLocalEngineConfig.enable_spark_context_inplace: True}):
                task_instance = word_count_inline.t(text=__file__)
                assert_run_task(task_instance)
```

 You can wrap `SparkSession` creation using `pytest.fixtures`.