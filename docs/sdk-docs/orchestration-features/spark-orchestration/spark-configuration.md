---
"title": "Spark Configuration"
---
Every Spark task has a `spark_engine` parameter that controls what Spark engine is used and a `spark_config` parameter that controls generic Spark configuration.

You can set global values for all spark tasks in the pipeline using your [environment configuration](doc:environment-configuration)
 
For example,  the `local` environment uses local `spark_submit` by default, while the `aws` environment uses `emr`. 
You can override the default `spark_engine` as a configuration setting or for any given run of the Spark task/pipeline


## Configure DBND Spark Engine
>ℹ️ Note
> To use remote Spark engines, you must have `dbnd-airflow` installed. 
To install `dbnd-airflow`, run `pip install dbnd-airflow`.

DBND supports the following Spark Engines:
* Local Spark via spark_submit
* [AWS EMR](https://aws.amazon.com/emr/)
* [GCP DataProc](https://cloud.google.com/dataproc)
* [Databricks](https://databricks.com/)
* [Qubole](https://www.qubole.com/)
* Any Spark server via [Apache Livy](https://livy.incubator.apache.org/) (ie. Cloudera)

  
## Configuring Spark Configuration

Regardless of the engine type, numerous parameters are used to control the submission of a Spark job as described [here](https://spark.apache.org/docs/latest/submitting-applications.html). The most common parameters include an amount of memory/CPU per job, or additional JAR/EGG files. Each spark task has a `SparkConfig` object associated with it. You can change these parameters using the ```SparkConfig``` object. 

  
The `SparkConfig` object can be mutated in the following ways:

1. Adding values into a configuration file under `[spark]` section. This would affect all running spark tasks:

``` ini
[spark]
jars = ['${DBND_HOME}/databand_examples/tool_spark/spark_jvm/target/lib/jsoup-1.11.3.jar']
main_jar =${DBND_HOME}/databand_examples/tool_spark/spark_jvm/target/ai.databand.examples-1.0-SNAPSHOT.jar
driver_memory = 2.5g
```

2. Override config value as part of the task definition:

``` python
class PrepareData(SparkTask):
    text = parameter.data
    counters = output

    main_class = "org.predict_wine_quality.PrepareData"
    # overides value of SparkConfig object
    defaults = {SparkConfig.driver_memory: "2.5g", "spark.executor_memory" : "1g"}

    def application_args(self):
        return [self.text, self.counters]
``` 

3. From command-line:

``` bash
dbnd run PrepareData --set spark.executor_memory 2.5g  --extend spark.conf={"spark.driver.memoryOverhead": "4G"}
```
 
| Parameter | Description |
|---|---|
|      spark.main_jar     |      Main application jar     |
|      spark.driver_classpath     |      Additional, driver-specific, classpath settings     |
|      spark.jars     |      Submit additional jars to upload and place them in executor classpath.     |
|      spark.py_files     |      Additional Python files used by the job; can be .zip, .egg or .py.     |
|      spark.files     |      Upload additional files to the executor running the job, separated by a comma.  Files will be placed in the working directory of each executor; for example, serialized objects.     |
|      spark.packages     |      A comma-separated list of maven coordinates of jars to include on the driver and executor classpaths.     |
|      spark.exclude_packages     |     A comma-separated list of maven coordinates of jars to exclude while resolving the dependencies provided in 'packages'.     |
|      spark.repositories     |    A comma-separated list of additional remote repositories to search for the maven coordinates given with 'packages'.   |
|      spark.conf     |      Arbitrary Spark configuration properties.  See [Extending Values](doc:extending-parameters-with-extend)     |
|      spark.num_executors     |      Number of executors to launch     |
|      spark.total_executor_cores     |      (Standalone & Mesos only) Total cores for all executors     |
|      spark.executor_cores     |      (Standalone & YARN only) Number of cores per executor     |
|      spark.executor_memory     |      Memory per executor (e.g. 1000M, 2G) (Default: 1G)     |
|      spark.driver_memody     |      Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)     |
|      spark.driver_cores     |      (Liby only) Number of cores in driver     |
|      spark.keytab     |      Full path to the file that contains the keytab.     |
|      spark.principal     |      The name of the Kerberos principal used for the keytab.     |
|      spark.env_vars     |      Environment variables for `spark-submit`. It supports yarn and k8s modes.     |
|      spark.verbose     |      Whether to pass the verbose flag to the `spark-submit` process for debugging.     |



To override specific task configuration, use `--set TASK_NAME.task_config="{ 'spark' { 'PARAMETER' : VALUE}}" `. For example: 
```
--set  PrepareData.task_config="{ 'spark' : {'num_executors' : 5} }"
```

Alternatively, you can also edit configuration from inside your code, or use environment variables. See more in [Defaults for Engines and Nested Tasks](doc:task-configuration-defaults).


### Q: Is it possible to edit `py_files` from CLI, without editing the project.cfg? Which environment variables can I set to change this configuration?
A: You can always run set specific configuration for each run: 
`dbnd run ….. --set spark.py_files=s3://…`

### Q: How to use multiple clusters in the same pipeline? 
A: You can change the configuration of the specific task via. `--set prepare_data.spark_engine=another_engine`. Make sure you have a definition of the engine.  You can also change task_config of some pipeline, so all internal tasks will have specific `spark_engine`. For example   --set prepare_data.task_config="{ 'some_qubole_engine' : {'cluster_label' : "another_label"} }"