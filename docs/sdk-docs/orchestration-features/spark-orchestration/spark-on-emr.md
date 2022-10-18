---
"title": "Spark on AWS EMR"
---
By default, DBND uses EMR step to submit Spark jobs to EMR.  The command line is going to be generated using SparkSubmitHook, thus airflow connection should be defined to be used by that hook.

## Configuring AWS EMR

1. Set `spark_engine` to `emr`
2. Define cluster ID in EMR configuration:
```ini
[emr]
cluster_id= < emr cluster name id >
```

3. Define airflow connection:
```shell
$airflow connections --delete --conn_id spark_emr
$airflow connections --add \
    --conn_id spark_emr \
    --conn_type docker \
    --conn_host local
```

## `[emr]` Configuration Section Parameter Reference
- `root` - Data outputs location override
- `disable_task_band` - Disable task_band file creation
- `cluster` - Set the cluster's name.
- `policy` - Determine the cluster's start/stop policy.
- `region` - Determine the region to use for the AWS connection.
- `conn_id` - Set Spark emr connection settings.
- `action_on_failure` - Set an action to take on failure of Spark submit. E.g. `CANCEL_AND_WAIT` or `CONTINUE`.
- `emr_completion_poll_interval` - Set the numbber of seconds to wait between polls of step completion job.
- `client` - Set the type of client used to run EMR jobs.
- `ssh_mode` - If this is enabled, use localhost:SSH_PORT(8998) to connect to livy.
- `livy_port` - Set which port will be used to connect to livy.


## Supporting Inline Spark Tasks on EMR
In order to support inline tasks, install the `dbnd` and `dbnd-spark` package on your EMR cluster nodes. See [Installing DBND on Spark Cluster](doc:installing-dbnd-on-spark-cluster) for more information.
