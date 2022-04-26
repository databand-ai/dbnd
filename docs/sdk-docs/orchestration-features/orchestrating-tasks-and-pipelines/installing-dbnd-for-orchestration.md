---
"title": "Installing SDK"
---
# Installing DBND for Orchestration

If you are looking for a tracking use-case, please check [Installing Python SDK for Tracking](doc:installing-dbnd)

From the command line, run the following command:
```shell
pip install databand
```

The DBND PyPI basic package installs only packages required for getting started. Behind the scenes, DBND does conditional imports of operators that require extra dependencies.

Whether you are looking to track your pipeline metadata, or if you want to orchestrate pipelines, you may want to install DBND plugins for integrating with third-party tools.

See [Connecting DBND to Databand](doc:access-token) to learn how to connect SDK integrations into your Databand Application.


## Installing Plugins

Run the following command to install any of the plugins listed in the tables below. For example:
```shell
pip install dbnd-spark dbnd-airflow
```

You can use bundled installation via `databand[plugin-slug]`
```shell
pip install databand[spark,airflow]
```


## Plugins for Orchestration


| Plugin name | Observability Mode |
|---|---|
| dbnd-airflow | Runs DBND pipeline with Airflow as a backend for orchestration (parallel/kubernetes modes). Functional operators by DBND in your Airflow DAGs definitions. This plugin is also required for installing cloud environments. |
| dbnd-airflow-versioned-dag | Allows execution of DAGs in Airflow that are versioned, so you can change your DAGs dynamically. This plugin also installs the Airflow plugin. |
| dbnd-aws | Enables integration with Amazon Web Services, S3, Amazon Batch, etc. |
| dbnd-azure | Enables integration with Microsoft Azure (DBFS, Azure, BLOB). |
| dbnd-databricks | Enables integration with Databricks via SparkTask. |
| dbnd-docker | Enables docker engine for task execution (DockerTask, Kubernetes, and Docker engines). |
| dbnd-gcp | Enables integration with Google Cloud Platform (GS, Dataproc, Dataflow, Apache_beam) |
| dbnd-hdfs | Enables integration with Hadoop File System. |
| dbnd-spark | Enables integration with Apache Spark distributed general-purpose cluster-computing framework. |
| dbnd-qubole | Enables integration with Qubole data lake platform. |
| dbnd-tensorflow | Enables integration with TensorFlow machine learning software. |




## Initiate storage and configurations


To create the default project structure, run the following command in any directory on the file system:

```shell
$ dbnd project-init
```

You will see the following log:
```shell
[2021-01-21 10:37:55,701] INFO - Databand project has been initialized at <YOUR PATH>
```

This command creates a project configuration file - **project.cfg**.

You can also initialize your project folder by manually creating `project.cfg` inside any directory or setting the `DBND_HOME` environment variable. `$DBND_HOME` refers to the project root directory.


> **Note**
>
> Running `dbnd project-init` overwrites the **project.cfg**.

## Test the installation

To make sure DBND is operational and ready to be used,  run `dbnd_sanity_check` pipeline, available in the DBND package:

```bash
dbnd run dbnd_sanity_check
```

If the command runs successfully, you will see the following message:

```bash
= Your run has been successfully executed!
 TASKS      : total=1  success=1
```

To check on how outputs, metrics, and metadata from the run are persisted, run the following command:

```shell
find <Project Path>/data/
```
