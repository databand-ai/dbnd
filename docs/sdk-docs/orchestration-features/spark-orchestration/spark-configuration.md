---
"title": "Spark Configuration"
---
Every Spark task has a `spark_engine` parameter that controls what Spark engine is used and a `spark_config` parameter that controls generic Spark configuration.

You can set global values for all spark tasks in the pipeline using your [environment configuration](doc:environment-configuration)

For example,  the `local` environment uses local `spark_submit` by default, while the `aws` environment uses `emr`.
You can override the default `spark_engine` as a configuration setting or for any given run of the Spark task/pipeline


## Configure DBND Spark Engine
>ℹ️ Note
> To use remote Spark engines, you must have `dbnd-run` installed.
To install `dbnd-run`, run `pip install dbnd-run`.

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

<!-- noqa -->
```python
from dbnd_spark import SparkTask, SparkConfig
from dbnd import parameter, output

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

To override specific task configuration, use `--set TASK_NAME.task_config="{ 'spark' { 'PARAMETER' : VALUE}}" `. For example:
```
--set  PrepareData.task_config="{ 'spark' : {'num_executors' : 5} }"
```

Alternatively, you can also edit configuration from inside your code, or use environment variables. See more in [Defaults for Engines and Nested Tasks](doc:task-configuration-defaults).

## `[spark]` Configuration Section Parameter Reference
- `main_jar` - Set the path to the main application jar.
- `driver_class_path` - Determine additional, driver-specific, classpath settings.
- `jars` - Submit additional jars to upload and place them in the executor classpath.
- `py_files` - Set any additional python files used by the job. This can be .zip, .egg or .py.
- `files` - Upload additional files to the executor running the job, separated by a comma. Files will be placed in the working directory of each executor. For example, serialized objects.
- `packages` - Set a comma-separated list of maven coordinates of jars to include on the driver and executor classpaths.
- `exclude_packages` - Comma-separated list of maven coordinates of jars to exclude while resolving the dependencies provided in `packages`.
- `repositories` - Comma-separated list of additional remote repositories to search for the maven coordinates given with `packages`.
- `conf` - Set arbitrary Spark configuration properties.
- `num_executors` - Determine the number of executors to launch.
- `total_executor_cores` - Set the number of total cores for all executors. This is only applicable for standalone and Mesos.
- `executor_cores` - Set the number of cores per executor. This is only applicable for Standalone and YARN.
- `status_poll_interval` - Set the number of seconds to wait between polls of driver status in the cluster.
- `executor_memory` - Set the amount of memory per executor, e.g. 1000M, 2G. The default value is 1G.
- `driver_memory` - Set the amount of memory allocated to the driver, e.g. 1000M, 2G. The default value is 1G.
- `driver_cores` - Set the number of cores in the driver. This is only applicable for Livy.
- `queue` - Set the YARN queue to submit to.
- `proxy_user` - Set the user to impersonate when submitting the application. This argument does not work with `--principal` or `--keytab`.
- `archives` - Set a comma separated list of archives to be extracted into the working directory of each executor.
- `keytab` - Set the full path to the file that contains the keytab.
- `principal` - Set the name of the Kerberos principal used for keytab.
- `env_vars` - Set the environment variables for spark-submit. It supports yarn and k8s mode too.
- `verbose` - Determine whether to pass the verbose flag to the spark-submit process for debugging
- `deploy_mode` - Set the driver mode of the spark submission.
- `submit_args` - Set spark arguments as a string, e.g. `--num-executors 10`
- `disable_sync` - Disable databand auto-sync mode for Spark files.
- `disable_tracking_api` - Disable saving metrics and DataFrames (so log_metric and log_dataframe will just print to the spark log). Set this to true if you can't configure connectivity from the Spark cluster to the databand server.
- `use_current_spark_session` - If Spark Session exists, do not send to remote cluster/spark-submit, but use existing.
- `listener_inject_enabled` - Enable Auto-injecting Databand Spark Listener. This listener will record and report spark metrics to the databand server.
- `include_user_project` - Enable building fat_wheel from configured package and third-party requirements (configured in bdist_zip section) and upload it to Spark
- `fix_pyspark_imports` - Determine whether databand should reverse import resolution order when running within spark.
- `disable_pluggy_entrypoint_loading` - When set to true, databand will not load any plugins within spark execution, other than the plugins loaded during spark submission.

## FAQ

### Q: Is it possible to edit `py_files` from CLI, without editing the project.cfg? Which environment variables can I set to change this configuration?
A: You can always run set specific configuration for each run:
`dbnd run ….. --set spark.py_files=s3://…`

### Q: How to use multiple clusters in the same pipeline?
A: You can change the configuration of the specific task via. `--set prepare_data.spark_engine=another_engine`. Make sure you have a definition of the engine.  You can also change task_config of some pipeline, so all internal tasks will have specific `spark_engine`. For example   --set prepare_data.task_config="{ 'some_qubole_engine' : {'cluster_label' : "another_label"} }"

**`[qubole]` Configuration Section Parameter Reference**
- `root` - Data outputs location override
- `disable_task_band` - Disable task_band file creation
- `cloud` - What cloud to be used. The default value for this is `AWS`
- `api_url` - Set the API URL without a version. e.g. `https://<ENV>.qubole.com/api`
- `ui_url` - Set the UI URL for accessing Qubole logs.
- `api_token` - Set the API key of the qubole account.
- `cluster_label` - Set the label of the cluster to run the command on.
- `status_polling_interval_seconds` - Determine the number of seconds to sleep between polling databricks for job status.
- `show_spark_log` - If True, full spark log will be printed.
- `qds_sdk_logging_level` - Determine qubole's sdk log level.

