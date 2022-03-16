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