---
"title": "Getting Started with DBND Tracking"
---
DBND tracking allows you to track pipelines and associated data, both holistically and atomically. With seamless integrations, it is easy to track and create alerts for metadata, pipeline performance, query resource usage, and custom metrics. 

This [Python Quickstart](doc:tracking-quickstart) will walk you through some basic Databand capabilities with Python script as an example. 

Pipeline metadata describes a comprehensive range of metadata used for tracking and monitoring, uniquely relevant to active data processes. In other words, pipeline metadata includes any system, application, graph process, or data level information that’s broadly relevant to the normal functioning of your data pipelines. This includes:

* Job Runtime Information
* Application Logs
* Task Function Statuses
* Data Quality Metrics
* Input/Output
* Data Lineage

DBND tracks this information and contextualizes it in your pipeline and task definitions, so you can instantly see for any given pipeline where issues are coming from, and from a system-wide perspective which pipelines are the source of problems.

# Methods of Use

Once tracking is enabled, you can use functions like [log_dataset_op](doc:tracking-python-datasets)  to track Pandas or Spark dataframes and [`log_metric`](doc:metrics)  to track custom metrics or key performance indicators in your various tasks. With these tracking methods, histograms, statistics, previews, and more can be observed directly in your CLI or if you are using Apache Airflow, in your `airflow` logs. 

Tracking plugins extend the native tracking features of orchestrators such as [Apache Airflow](doc:tracking-airflow-dags)  and [Azkaban](doc:tracking-azkaban), data lake providers such as Snowflake and Redshift, and more. For a full list of plugins, visit [Installing DBND Plugins](doc:installing-dbnd#plugins-for-tracking). 

While these logging methods provide a more atomic approach to tracking pipelines, metadata, and data, you can also integrate tracking holistically to minimize code overhead.


# Current Integrations
The current list of integrations available for tracking with Databand includes:

###Supported Languages
  * [Python](doc:python) 
  * [Java](doc:jvm)
  * [Scala](doc:jvm)
 
###[Tracking Apache Spark](doc:tracking-spark-applications)
  * [AWS EMR ](doc:tracking-spark-applications)
  * [GCP Dataproc](doc:tracking-spark-applications)
  * [Local Spark](doc:tracking-spark-applications)
  * [Databricks](doc:tracking-spark-applications)
  * [Qubole](doc:tracking-spark-applications)
  * Any Spark server via Apache Livy

###Tracking Databases
  * [Amazon Redshift](doc:tracking-redshift)  
  * [Snowflake](doc:tracking-snowflake-guide) 
  * [Tracking BigQuery](doc:tracking-bigquery) 


### [Tracking Apache Airflow](doc:tracking-airflow-dags) 
* [Integrating with Standard Apache Airflow Cluster](doc:installing-on-airflow-cluster#standard-apache-airflow-cluster)
* [Integrating Amazon Managed Workflows Airflow](doc:installing-on-airflow-cluster#aws-managed-workflows)
* [Integrating Google Cloud Composer](doc:installing-on-airflow-cluster#google-cloud-composer)
* [Integrating Astronomer](doc:installing-on-airflow-cluster#astronomer)

###Tracking Other Orchestrators
  * [Azkaban](doc:tracking-azkaban)
  * [DBND](doc:orchestrating-tasks-and-pipelines) 
  * Luigi 

###Tracking Other Trackers 
  * [MLFlow](doc:tracking-mlflow)