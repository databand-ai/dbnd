---
"title": "Runtime Environment Configuration"
---
## Environments Overview
DBND provides out-of-the-box environments that you need to configure before you can start running your pipelines. 
Out-of-the-box, DBND supports the following environment types:
* Persistency - Local file system, AWS S3, Google Storage, Azure Blob Store, and HDFS
* Spark - Local Spark, Amazon EMR, Google DataProc, Databricks, Qubole, and Livy
* Docker Engine - Local Docker, AWS Batch, Kubernetes.
You can also create custom environments and engines.

 
## Main Configuration 
   
• The environments parameter in the `core` section specifies a list of environments enabled and available for the project. 
Possible values include ` local, gcp, aws, azure`, and `local` - set by default.

```ini 
[core]
environments = ['local', 'gcp']
```


The following describe the environment types supported in DBND:

### Local
In the default `local` environment setup, the configuration works as follows: <br>A persistent metadata store for task inputs/outputs, metrics, and runtime execution information is set to the local file system under `$DBND_HOME/data`. <br>Python tasks run as processes on the local machine. <br>Spark tasks run locally by using the `spark_submit` command if you have local Spark in place. <br>Docker tasks run on your local Docker container.

### Google Cloud Platform (GCP), out-of-the-box
The Spark engine is preset for Google DataProc.  To set up a GCP environment, you will need to provide a GS bucket as a root path for the metadata store and the Airflow connection ID with cloud authentication info.
See [Setting up GCP Environment.](doc:gcp-environment).

### Amazon Web Service (AWS), out-of-the-box
The Spark engine is preset for Amazon EMR.  To set an AWS environment, you need to provide an S3 bucket as a root path for the metadata store and the Airflow connection ID with cloud authentication information.
See [Setting Up an AWS Environment](doc:aws-environment).

### Microsoft Azure (Azure), out-of-the-box
The Spark engine is preset for Databricks.  To set up an Azure environment, you need to provide an Azure Blob Store bucket as a root path for the metadata store and the Airflow connection ID with cloud authentication information. 
See [Setting Up an Azure Environment](doc:azure-environment).

### Custom environment
You can define a custom environment from scratch or inherit settings from an existing environment.  You can create custom environments for managing dev/staging/production lifecycles, which normally involves switching data and execution locations.
See [Setting up a Custom Environment (Extending Configurations)](doc:extending-configurations).


To use out-of-the-box environments, set up one or more of the environments described in the referenced topics.