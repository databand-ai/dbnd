---
"title": "Spark on GCP Dataproc"
---
You can use Google Dataproc cluster to run your jobs. This requires you to configure a GCP connection:

```bash
airflow connections --add \
--conn_id google_cloud_default \
--conn_type google_cloud_platform \
--conn_extra "{\"extra__google_cloud_platform__key_path\": \"<PATH/TO/KEY.json>\", \"extra__google_cloud_platform__project\": \"<PROJECT_ID>\"}"
```

Then you need to adjust your dbnd config:

```ini
[gcp]
_type = dbnd_gcp.env.GcpEnvConfig
dbnd_local_root = ${DBND_HOME}/data/dbnd
root = gs://<YOUR-BUCKET>
conn_id = google_cloud_default
spark_engine = dataproc

[dataproc]
region = <REGION>
zone = <ZONE>
num_workers = 0
master_machine_type = n1-standard-1
worker_machine_type = n1-standard-1
cluster = <CLUSTER-NAME>
```

Once your configuration is ready you can use the cluster:

```sh
dbnd run dbnd_sanity_check --env gcp
```

## `[dataproc]` Configuration Section Parameter Reference
- `root` - Data outputs location override
- `disable_task_band` - Disable task_band file creation
- `cluster` - Set the cluster's name.
- `policy` - Set the cluster's start/stop policy.
- `region` - Determine the GCP region to be used.
- `zone` - Set the zone where the cluster will be located. This is templated.
- `num_workers` - Determine the number of workers to spin up.
- `num_preemptible_workers` - Determine the number of preemptible worker nodes to spin up.
- `network_uri` - Set the network uri to be used for machine communication. This cannot be specified with subnetwork_uri.
- `subnetwork_uri` - The subnetwork uri to be used for machine communication. This cannot be specified with network_uri.
- `tags` - Sety the GCE tags to add to all instances.
- `storage_bucket` - Set which storage bucket will be used. Setting this to `None` lets dataproc generate a custom one for you.
- `init_actions_uris` - List of GCS uri's containing dataproc initialization scripts
- `init_action_timeout` - Set how much time executable scripts in `init_actions_uris` have to complete
- `metadata` - Set a dictionary of key-value google compute engine metadata entries to add to all instances.
- `image_version` - Determine the version of the software inside the Dataproc cluster.
- `properties` - Set dictionary of properties to set on config files (e.g. spark-defaults.conf), see https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#SoftwareConfig
- `master_machine_type` - Compute engine machine type to use for the master node.
- `master_disk_size` - Determine the disk size for the master node.
- `worker_machine_type` - Compute engine machine type to use for the worker nodes.
- `worker_disk_size` - Determine the disk size for the worker nodes.
- `delegate_to` - Set which account to impersonate, if any. For this to work, the service account making the request must have domain-wide delegation enabled.
- `service_account` - Set the service account of the dataproc instances.
- `service_account_scopes` - Set the URIs of service account scopes to be included.
- `idle_delete_ttl` - Determine the longest duration, in seconds, that the cluster would keep alive while staying idle. Passing this threshold will cause the cluster to be auto-deleted.
- `auto_delete_time` - Set the time when the cluster will be auto-deleted.
- `auto_delete_ttl` - Determine the life duration, in seconds, of the cluster. The cluster will be auto-deleted at the end of this duration. If auto_delete_time is set this parameter will be ignored

