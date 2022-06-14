---
"title": "Installing on Spark Cluster"
---
## General Installation
Make sure that the Databand Server is accessible from your Spark Cluster.

## JVM Integration
The following environment variables should be defined in your Spark context.
 * `DBND__CORE__DATABAND_URL` - a  Databand server URL
 * `DBND__CORE__DATABAND_ACCESS_TOKEN` - a  Databand server Access Token
 * `DBND__TRACKING=True`
Please see [Installing JVM SDK and Agent](doc:installing-jvm-dbnd) for detailed information on all available parameters. Your cluster should have Databand .jars to be able to use Listener and other features. See below.

## Python Integration
The following environment variables should be defined in your Spark context.
 * `DBND__CORE__DATABAND_URL` - a  Databand server URL
 * `DBND__CORE__DATABAND_ACCESS_TOKEN` - a  Databand server Access Token
 * `DBND__TRACKING=True`
 * `DBND__ENABLE__SPARK_CONTEXT_ENV=True`
You should install `dbnd-spark`. See [Installing Python SDK](doc:installing-dbnd) and bootstrap example below

# Cluster Bootstrap
Most of the Spark clusters support bootstrap scripts (EMR, Dataproc, and others).

## Configure Tracking Properties on Spark Cluster with Bootstrap script

Add the following commands to your cluster initialization script:
``` bash
#!/bin/bash -x

DBND_VERSION=REPLACE_WITH_DBND_VERSION

#Configure your Databand Tracking Configuration (works only for generic cluster/dataproc, not for EMR)
echo "export DBND__TRACKING=True" | tee -a /usr/lib/spark/conf/spark-env.sh
echo "export DBND__ENABLE__SPARK_CONTEXT_ENV=True" | tee -a /usr/lib/spark/conf/spark-env.sh
echo "export DBND__CORE__DATABAND_URL=REPLACE_WITH_DATABAND_URL" | tee -a /usr/lib/spark/conf/spark-env.sh
echo "export DBND__CORE__DATABAND_ACCESS_TOKEN=REPLACE_WITH_DATABAND_TOKEN" | tee -a /usr/lib/spark/conf/spark-env.sh

# if you use Listeners/Agent, download Databand Agent which includes all jars
wget https://repo1.maven.org/maven2/ai/databand/dbnd-agent/${DBND_VERSION}/dbnd-agent-${DBND_VERSION}-all.jar -P /home/hadoop/

# install Databand Python package together with Airflow support
python -m pip install databand[spark]==${DBND_VERSION}
```

* Be sure to replace `<databand-url>` and `<databand-access-token>` with your environment-specific information.
* Install the `dbnd` packages on the Spark master and Spark workers by running `pip install databand[spark]` at bootstrap or manually.
* Make sure you don't install "dbnd-airflow" to the cluster.


# Spark Clusters

## EMR Cluster

### Setting Databand Configuration via Environment Variables

You need to define Environment Variables at the API call/`EmrCreateJobFlowOperator` Airflow Operator.  An alternative way is to provide all these variables via AWS UI where you create a new cluster. EMR cluster doesn't have a way of defining Environment Variables in the bootstrap.

<!-- noqa -->
```python
from airflow.hooks.base_hook import BaseHook

dbnd_config = BaseHook.get_connection("dbnd_config").extra_dejson
databand_url = dbnd_config["core"]["databand_url"]
databand_access_token = dbnd_config["core"]["databand_access_token"]

emr_create_job_flow = EmrCreateJobFlowOperator(
    job_flow_overrides={
        "Name": "<EMR Cluster Name>",
         ...
        "Configurations": [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "DBND__TRACKING": "True",
                            "DBND__ENABLE__SPARK_CONTEXT_ENV": "True",
                            "DBND__CORE__DATABAND_URL": databand_url,
                            "DBND__CORE__DATABAND_ACCESS_TOKEN": databand_access_token,
                        },
                    }
                ],
            }
        ]
    }
    ...
)

```

### Installing Databand on Cluster

As EMR cluster has built-in support for bootstrap scripts, please, follow bootstrap option documentation to install python and JVM integrations.


## Databricks Cluster
### Setting Databand Configuration via Environment Variables

In the cluster configuration screen, click 'Edit'>>Advanced Options>>Spark.
Inside the Environment Variables section, declare the configuration variables listed below. Be sure to replace `<databand-url>` and `<databand-access-token>` with your environment specific information:

* `DBND__TRACKING="True"`
* `DBND__ENABLE__SPARK_CONTEXT_ENV="True"`
* `DBND__CORE__DATABAND_URL="REPLACE_WITH_DATABAND_URL"`
* `DBND__CORE__DATABAND_ACCESS_TOKEN="REPLACE_WITH_DATABAND_TOKEN"`


### Install Python DBND library in Databricks cluster

Under the Libraries tab of your cluster's configuration:
* Click 'Install New'
* Choose the PyPI option
* Enter `databand[spark]==REPLACE_WITH_DBND_VERSION` as the Package name
* Click 'Install'


### Install Python DBND library for specific Airflow Operator
| Do not use this mode in production, use it only for trying out DBND in specific Task.
Ensure that the `dbnd` library is installed on the Databricks cluster by adding `databand[spark]` to the `libraries` parameter of the `DatabricksSubmitRunOperator`, shown in the example below:

<!-- noqa -->
```python
DatabricksSubmitRunOperator(
     ...
     json={"libraries": [
        {"pypi": {"package": "databand[spark]==REPLACE_WITH_DBND_VERSION"}},
    ]},
)
```

### Tracking Scala/Java Spark Jobs
Download [DBND Agent](doc:installing-jvm-dbnd#dbnd-jvm-agent) and place it into your DBFS working folder.

To configure [Tracking Spark Applications](doc:tracking-spark-applications) with automatic dataset logging, add `ai.databand.spark.DbndSparkQueryExecutionListener` as a  `spark.sql.queryExecutionListeners` (this mode works only if DBND  agent has been enabled)

Use the following configuration of the Databricks job to enable Databand Java Agent with automatic dataset tracking:

<!-- noqa -->
```python
spark_operator = DatabricksSubmitRunOperator(
    json={
        ...
        "new_cluster": {
             ...
            "spark_conf": {
                "spark.sql.queryExecutionListeners": "ai.databand.spark.DbndSparkQueryExecutionListener",
                "spark.driver.extraJavaOptions": "-javaagent:/dbfs/apps/dbnd-agent-0.xx.x-all.jar",
            },
        },
        ...
    })
```
Make sure that you have published the agent to `/dbfs/apps/` first
For more configuration options, see the Databricks [Runs Submit API documentation](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit).


## GoogleCloud DataProc Cluster
### Cluster Setup

You can define environment variables during the cluster setup or add these variables to your bootstrap as described at [Installing on Spark Cluster](doc:installing-dbnd-on-spark-cluster) :


<!-- noqa -->
```python
from airflow.hooks.base_hook import BaseHook

dbnd_config = BaseHook.get_connection("dbnd_config").extra_dejson
databand_url = dbnd_config["core"]["databand_url"]
databand_access_token = dbnd_config["core"]["databand_access_token"]

cluster_create = DataprocClusterCreateOperator(
     ...
     properties={
        "spark-env:DBND__TRACKING": "True",
        "spark-env:DBND__ENABLE__SPARK_CONTEXT_ENV": "True",
        "spark-env:DBND__CORE__DATABAND_URL": databand_url,
        "spark-env:DBND__CORE__DATABAND_ACCESS_TOKEN": databand_access_token,
    },
    ...
)
```
You can install Databand PySpark support via the same operator:

<!-- noqa -->
```python
cluster_create = DataprocClusterCreateOperator(
     ...
     properties={
             "dataproc:pip.packages": "dbnd-spark==REPLACE_WITH_DATABAND_VERSION",
     }
      ...
)
```

As Dataproc cluster has built-in support for bootstrap scripts, please, follow bootstrap option documentation to enable python and JVM integrations via bootstrap [Installing on Spark Cluster](doc:installing-dbnd-on-spark-cluster#configure-jvm-tracking-properties-on-spark-cluster-via-bootstrap)


### Tracking Python Spark Jobs
Use the following configuration of the PySpark DataProc job to enable Databand SparkQuery Listener with automatic dataset tracking:

<!-- noqa -->
```python
pyspark_operator = DataProcPySparkOperator(
    ...
    dataproc_pyspark_jars=[ "gs://.../dbnd-agent-REPLACE_WITH_DATABAND_VERSION-all.jar"],
    dataproc_pyspark_properties={
        "spark.sql.queryExecutionListeners": "ai.databand.spark.DbndSparkQueryExecutionListener",
     },
      ...
)
```
* You should [publish](doc:installing-jvm-dbnd#manual) your jar to Google Storage first.

See the list of all supported operators and extra information at [Tracking Subprocess/Remote Tasks](doc:tracking-airflow-subprocess-remote) section.

## Next Steps

See the [Tracking Python](doc:python) section for implementing `dbnd` within your PySpark jobs. See the [Tracking Spark/JVM Applications](doc:tracking-spark-applications) for Spark/JVM jobs
[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>\t\t"
}
[/block]