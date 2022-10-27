---
"title": "Installing on Spark Cluster"
---

## General Installation

Make sure that the Databand Server is accessible from your Spark Cluster.

## JVM Integration

The following environment variables should be defined in your Spark context.

-   `DBND__CORE__DATABAND_URL` - a Databand server URL
-   `DBND__CORE__DATABAND_ACCESS_TOKEN` - a Databand server Access Token
-   `DBND__TRACKING=True`
    Please see [Installing JVM SDK and Agent](doc:installing-jvm-dbnd) for detailed information on all available parameters. Your cluster should have Databand .jars to be able to use Listener and other features. See below.

## Python Integration

The following environment variables should be defined in your Spark context.

-   `DBND__CORE__DATABAND_URL` - a Databand server URL
-   `DBND__CORE__DATABAND_ACCESS_TOKEN` - a Databand server Access Token
-   `DBND__TRACKING=True`
-   `DBND__ENABLE__SPARK_CONTEXT_ENV=True`

You should install `dbnd-spark`. See [Installing Python SDK](doc:installing-dbnd) and bootstrap example below

# Spark Clusters

Most clusters support setting up Spark environment variables via cluster metadata or via bootstrap scripts. Below are provider-specific instructions.

## EMR Cluster

### Setting Databand Configuration via Environment Variables

You need to define Environment Variables at the API call or `EmrCreateJobFlowOperator` Airflow Operator. An alternative way is to provide all these variables via AWS UI where you create a new cluster. EMR cluster doesn't have a way of defining Environment Variables in the bootstrap. Please consult with official [EMR documentation on Spark Configuration](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html) if you use custom operator or creating cluster outside Ariflow.

<!-- noqa -->

```python
from airflow.hooks.base_hook import BaseHook

dbnd_config = BaseHook.get_connection("dbnd_config").extra_dejson
databand_url = dbnd_config["core"]["databand_url"]
databand_access_token = dbnd_config["core"]["databand_access_token"]

emr_create_job_flow = EmrCreateJobFlowOperator(
    job_flow_overrides={
        "Name": "<EMR Cluster Name>",
        #...
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
    #...
)

```

### Installing Databand on Cluster

As EMR cluster has support for bootstrap actions, following snippet can be used to install python and jvm integrations:

```shell
#!/usr/bin/env bash

DBND_VERSION=REPLACE_WITH_DBND_VERSION

sudo python3 -m pip install pandas==1.2.0 pydeequ==1.0.1 databand[spark]==${DBND_VERSION}
sudo wget https://repo1.maven.org/maven2/ai/databand/dbnd-agent/${DBND_VERSION/dbnd-agent-${DBND_VERSION}-all.jar -P /home/hadoop/
```

Add this script to your cluster bootstrap actions list. For more details please, please follow [bootstrap actions documentation](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html).

## Databricks Cluster

### Setting Databand Configuration via Environment Variables

In the cluster configuration screen, click 'Edit'>>'Advanced Options'>>'Spark'.
Inside the Environment Variables section, declare the configuration variables listed below. Be sure to replace `<databand-url>` and `<databand-access-token>` with your environment specific information:

-   `DBND__TRACKING="True"`
-   `DBND__ENABLE__SPARK_CONTEXT_ENV="True"`
-   `DBND__CORE__DATABAND_URL="REPLACE_WITH_DATABAND_URL"`
-   `DBND__CORE__DATABAND_ACCESS_TOKEN="REPLACE_WITH_DATABAND_TOKEN"`

### Install Python DBND library in Databricks cluster

Under the Libraries tab of your cluster's configuration:

-   Click 'Install New'
-   Choose the PyPI option
-   Enter `databand[spark]==REPLACE_WITH_DBND_VERSION` as the Package name
-   Click 'Install'

### Install Python DBND library for specific Airflow Operator

| Do not use this mode in production, use it only for trying out DBND in specific Task.
Ensure that the `dbnd` library is installed on the Databricks cluster by adding `databand[spark]` to the `libraries` parameter of the `DatabricksSubmitRunOperator`, shown in the example below:

<!-- noqa -->

```python
DatabricksSubmitRunOperator(
     #...
     json={"libraries": [
        {"pypi": {"package": "databand[spark]==REPLACE_WITH_DBND_VERSION"}},
    ]},
)
```

### Tracking Scala/Java Spark Jobs

Download [DBND Agent](doc:installing-jvm-dbnd#dbnd-jvm-agent) and place it into your DBFS working folder.

To configure [Tracking Spark Applications](doc:tracking-spark-applications) with automatic dataset logging, add `ai.databand.spark.DbndSparkQueryExecutionListener` as a `spark.sql.queryExecutionListeners` (this mode works only if DBND agent has been enabled)

Use the following configuration of the Databricks job to enable Databand Java Agent with automatic dataset tracking:

<!-- noqa -->

```python
spark_operator = DatabricksSubmitRunOperator(
    json={
        #...
        "new_cluster": {
            #...
            "spark_conf": {
                "spark.sql.queryExecutionListeners": "ai.databand.spark.DbndSparkQueryExecutionListener",
                "spark.driver.extraJavaOptions": "-javaagent:/dbfs/apps/dbnd-agent-0.xx.x-all.jar",
            },
        },
        #...
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
     # ...
     properties={
        "spark-env:DBND__TRACKING": "True",
        "spark-env:DBND__ENABLE__SPARK_CONTEXT_ENV": "True",
        "spark-env:DBND__CORE__DATABAND_URL": databand_url,
        "spark-env:DBND__CORE__DATABAND_ACCESS_TOKEN": databand_access_token,
    },
    # ...
)
```

You can install Databand PySpark support via the same operator:

<!-- noqa -->

```python
cluster_create = DataprocClusterCreateOperator(
     #...
     properties={
             "dataproc:pip.packages": "dbnd-spark==REPLACE_WITH_DATABAND_VERSION",  # pragma: allowlist secret
     }
     #...
)
```

Dataproc cluster has support for initialization actions. Following script installs Databand libraries and set up environment variables required for tracking:

```shell
#!/usr/bin/env bash

DBND_VERSION=REPLACE_WITH_DBND_VERSION

# to use conda-provided python instead of system one
export PATH=/opt/conda/default/bin:${PATH}

python3 -m pip install pydeequ==1.0.1 databand[spark]==${DBND_VERSION}

DBND__CORE__DATABAND_ACCESS_TOKEN=$(/usr/share/google/get_metadata_value attributes/DBND__CORE__DATABAND_ACCESS_TOKEN)
sh -c "echo DBND__CORE__DATABAND_ACCESS_TOKEN=${DBND__CORE__DATABAND_ACCESS_TOKEN} >> /usr/lib/spark/conf/spark-env.sh"
DBND__CORE__DATABAND_URL=$(/usr/share/google/get_metadata_value attributes/DBND__CORE__DATABAND_URL)
sh -c "echo DBND__CORE__DATABAND_URL=${DBND__CORE__DATABAND_URL} >> /usr/lib/spark/conf/spark-env.sh"
sh -c "echo DBND__TRACKING=True >> /usr/lib/spark/conf/spark-env.sh"
sh -c "echo DBND__ENABLE__SPARK_CONTEXT_ENV=True >> /usr/lib/spark/conf/spark-env.sh"
```

Note that variables like access token and tracker url should be passed to the initialization action via cluster metadata properties. Please refer to [official Dataproc documentation](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions#passing_arguments_to_initialization_actions) for details.

### Tracking Python Spark Jobs

Use the following configuration of the PySpark DataProc job to enable Databand Spark Query Listener with automatic dataset tracking:

<!-- noqa -->

```python
pyspark_operator = DataProcPySparkOperator(
    #...
    dataproc_pyspark_jars=[ "gs://.../dbnd-agent-REPLACE_WITH_DATABAND_VERSION-all.jar"],
    dataproc_pyspark_properties={
        "spark.sql.queryExecutionListeners": "ai.databand.spark.DbndSparkQueryExecutionListener",
    },
    #...
)
```

-   You should [publish](doc:installing-jvm-dbnd#manual) your jar to Google Storage first.

See the list of all supported operators and extra information at [Tracking Subprocess/Remote Tasks](doc:tracking-airflow-subprocess-remote) section.

# Cluster Bootstrap

If you are using custom cluster installation, you have to install Databand packages, agent and configure environment variables for tracking.

Add the following commands to your cluster initialization script:

```bash
#!/bin/bash -x

DBND_VERSION=REPLACE_WITH_DBND_VERSION

# Configure your Databand Tracking Configuration (works only for generic cluster/dataproc, not for EMR)
sh -c "echo DBND__TRACKING=True >> /usr/lib/spark/conf/spark-env.sh"
sh -c "echo DBND__ENABLE__SPARK_CONTEXT_ENV=True >> /usr/lib/spark/conf/spark-env.sh"


# if you use Listeners/Agent, download Databand Agent which includes all jars
wget https://repo1.maven.org/maven2/ai/databand/dbnd-agent/${DBND_VERSION}/dbnd-agent-${DBND_VERSION}-all.jar -P /home/hadoop/

# install Databand Python package together with Airflow support
python -m pip install databand[spark]==${DBND_VERSION}
```

-   Install the `dbnd` packages on the Spark master and Spark workers by running `pip install databand[spark]` at bootstrap or manually.
-   Make sure you don't install "dbnd-airflow" to the cluster.

## How to provide Databand credentials via cluster bootstrap

In case you cluster type support configuring environment variables via a bootstrap script, you can use your bootstrap script to define Databand Credentials on cluster level:

```bash
sh -c "echo DBND__CORE__DATABAND_URL=REPLACE_WITH_DATABAND_URL >> /usr/lib/spark/conf/spark-env.sh"
sh -c "echo DBND__CORE__DATABAND_ACCESS_TOKEN=REPLACE_WITH_DATABAND_TOKEN >> /usr/lib/spark/conf/spark-env.sh"
```

-   Be sure to replace `<databand-url>` and `<databand-access-token>` with your environment-specific information.

## Databand Agent Path and Query Listener configuration for Spark Operators

Databand can automatically alter `spark-submit` command for variety for Spark operators and inject agent jar into the classpath and enable Query Listener. Following options are suitable for configuration in `dbnd_config` airflow connection:

```json
{
    "tracking_spark": {
        "query_listener": true,
        "agent_path": "/home/hadoop/dbnd-agent-latest-all.jar",
        "jar_path": null
    }
}
```

-   `query_listener` — enables Databand Spark Query Listener for auto-capturing dataset operations from Spark jobs.
-   `agent_path` — path to the Databand Java Agent FatJar. If provided, Databand will include this agent into Spark Job via `spark.driver.extraJavaOptions` configuration option. Agent is required if you want to track Java/Scala jobs annotated with `@Task`. Agent has to be placed in a cluster local filesystem for proper functioning.
-   `jar_path` — path to the Databand Java Agent FatJar. If provided, Databand will include jar into Spark Job via `spark.jars` configuration option. Jar can be placed in a local filesystem as well as S3/GCS/DBFS path.

Properties can be configured via environment variables or .cfg files. Please refer to the [SDK Configuration](doc:dbnd-sdk-configuration) for details.

## Next Steps

See the [Tracking Python](doc:python) section for implementing `dbnd` within your PySpark jobs. See the [Tracking Spark/JVM Applications](doc:tracking-spark-applications) for Spark/JVM jobs
[block:html]
{
"html": "<style>\n pre {\n border: 0.2px solid #ddd;\n border-left: 3px solid #c796ff;\n color: #0061a6;\n }\n\n.CodeTabs_initial{\n /_ box shadows with with legacy browser support - just in case _/\n -webkit-box-shadow: 0 10px 6px -6px #777; /_ for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ _/\n -moz-box-shadow: 0 10px 6px -6px #777; /_ for Firefox 3.5 - 3.6 _/\n box-shadow: 0 10px 6px -6px #777;/_ Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 _/\n }\n</style>\t\t"
}
[/block]
