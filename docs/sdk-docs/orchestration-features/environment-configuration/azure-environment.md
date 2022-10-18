---
"title": "Azure Environment"
---
## Before You Begin
You must have `dbnd-azure` plugin installed.

## To Set up an Environment for Microsoft Azure
1. Open the **project.cfg** file and add `Azure` to the list of environments.
2. Review the `[azure]` section and configure it in accordance with your login settings.
3. Optionally, provide a root bucket/folder for your data:

```shell
[azure]
root = https://<your_account>.blob.core.windows.net/dbnd
spark_engine = databricks
```

4. Configure access to your Blob storage:

```shell
dbnd airflow connections --delete --conn_id=azure_blob_storage_default
dbnd airflow connections --add --conn_id=azure_blob_storage_default --conn_login=<acount name> --conn_type=wasb --conn_password=<acount key>
```

## `[azure]` Configuration Section Parameter Reference
- `env_label` - Set the environment type to be used. E.g. dev, int, prod.
- `production` - This indicates that the environment is production.
- `conn_id` - Set the cloud connection settings.
- `root` - Determine the main data output location.
- `local_engine` - Set which engine will be used for local execution
- `remote_engine` - Set the remote engine for the execution of driver/tasks
- `submit_driver` - Enable submitting driver to `remote_engine`.
- `submit_tasks` - Enable submitting tasks to remote engine one by one.
- `spark_config` - Determine the Spark Configuration settings
- `spark_engine` - Set the cluster engine to be used. E.g. local, emr (aws), dataproc (gcp), etc.
- `hdfs` - Set the Hdfs cluster configuration settings
- `beam_config` - Set the Apache Beam configuration settings
- `beam_engine` - Set the Apache Beam cluster engine. E.g. local or dataflow.
- `docker_engine` - Set the Docker job engine, e.g. docker or aws_batch

