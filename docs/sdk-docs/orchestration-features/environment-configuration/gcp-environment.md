---
"title": "GCP Environment"
---
## Before You Begin
You must have the `dbnd-gcp` plugin installed.

## To Set up an Environment for Google Cloud Platform
1. Open the **project.cfg** file, and add `gcp` to the list of environments.
```ini
[core]
environments = ['local', 'gcp']
```

2. In the` [gcp]` section, set your Airflow connection ID, and optionally provide the root bucket/folder for your metadata store.
```ini
[gcp]
root = gs://databand_examples
conn_id =google_cloud_default
```

3. To configure the default connection with your Google Cloud project ID, in the command line run the following command:
```shell
$ dbnd airflow connections --add \
    --conn_id google_cloud_default \
    --conn_type google_cloud \
    --project_id <your project ID>
```

4. To use the default Google Cloud credentials, run the following command:
```shell
gcloud auth application-default login
```

## `[gcp]` Configuration Section Parameter Reference
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

