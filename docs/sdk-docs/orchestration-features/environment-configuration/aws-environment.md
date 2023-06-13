---
"title": "AWS Environment"
---
## Before You Begin
You must have the `dbnd-aws` plugin installed.

## To Set Up AWS Environment

1. Open the **project.cfg** file, and add `aws` to the list of environments.
2. In the `[aws]` section, configure your connection ID, and optionally provide a root bucket/folder for your data.

```ini
[run]
environments = ['local', 'aws']
...
[aws]
env_label = dev
root = s3://databand-playground/databand_project
region_name = us-east-2
```

3. To override the default `boto` credentials, used by default, run the following command:

```shell
$ dbnd airflow connections --add  \
        --conn_id aws_default \
        --conn_type S3 \
        --conn_extra "{\"aws_access_key_id\":\"_your_aws_access_key_id_\", \"aws_secret_access_key\": \"_your_aws_secret_access_key_\"}"
```

## `[aws]` Configuration Section Parameter Reference
- `env_label` - Set the environment type to be used. E.g. dev, int, prod.
- `production` - This indicates that the environment is production.
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
- `conn_id` - Set the connection id of AWS credentials / region name. If this is set to `None`,credential boto3 strategy will be used (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
- `region_name` - Set the region name to use in AWS Hook. Override the region_name in connection (if provided)
- `update_env_with_boto_creds` - Update the environment of the current process with boto credentials, so third party libraries like pandas can access s3.
