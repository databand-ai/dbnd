---
"title": "Spark on Databricks"
---
### Configuration for submission (dbnd driver) machine
  1.  Add `[databricks]` to your databand [configuration](doc:dbnd-sdk-configuration)
  2. Set `cluster_id` to databricks cluster id value
  3. Set `cloud_type = aws` or `cloud_type = azure`
  4. Configure your environment to use Databricks as spark_engine.
Example
```ini
 [aws_databricks]
_type = aws
spark_engine = databricks
.... additional configurations related to your environment
```
 5. Perform the following steps:

``` bash
airflow connections --delete --conn_id databricks_default
airflow connections --add \
--conn_id databricks_default \
--conn_type databricks \
--conn_host <YOUR DATABRICKS CLUSTER URI> \
--conn_extra "{\"token\": \"<YOUR ACCESS TOCKEN>\", \"host\": \" <YOUR DATABRICKS CLUSTER URI>\"}"
```

## `[databricks_aws]` Configuration Section Parameter Reference
- `ebs_count` - Number of nodes for spark machines.
- `aws_instance_profile_arn` - Set the IAM profile for spark machines.
- `aws_ebs_volume_type` - Set the EBS type.
- `aws_ebs_volume_count` - Set the number of EBS volumes.
- `aws_ebs_volume_size` - Set the size of EBS volume.




>📘 Getting Databricks Cluster Id
> * API: https://<CLUSTER_IP>/2.0/clusters/list
> * UI: Under clusters -> advanced options-> tags -> ClusterId

### Configuring Databricks Cluster

You can configure DBND to spin up a new cluster for every job or use an existing cluster (default behavior). You will need to install and configure the DBND package on the cluster. See  [Installing DBND on Databricks Spark Cluster](doc:installing-dbnd-on-spark-cluster) for more information

## `[databricks]` Configuration Section Parameter Reference
- `root` - Data outputs location override
- `disable_task_band` - Disable task_band file creation
- `cluster_id` - Set the ID of the existing cluster.
- `cloud_type` - Set the type of the cloud. This can either be AWS or Azure.
- `conn_id` - Set Databricks' connection ID.
- `connection_retry_limit` - Set the retry limit of the databricks connection.
- `connection_retry_delay` - Set the Databricks connection's delay in between retries.
- `status_polling_interval_seconds` - Set the number of seconds to sleep between polling databricks for a job's status.
- `cluster_log_conf` - Set the location that will be used for logs, e.g.: `{"s3": {"destination": "s3://<BUCKET>/<KEY>", "region": "us-east-1"}}`
- `num_workers` - Set the number of workers as in databricks' api.
- `init_scripts` - Set the init script list. The default for this is `{ 's3': { 'destination' : 's3://init_script_bucket/prefix', 'region' : 'us-west-2' } }`
- `spark_version` - Set the Spark version that will be used.
- `spark_conf` - Determine Spark's configuration settings.
- `node_type_id` - Nodes for spark machines
- `spark_env_vars` - Set Spark's environment variables.

