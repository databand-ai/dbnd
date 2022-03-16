---
"title": "GCP Environment"
---
## Before You Begin
You must have the `dbnd-gcp` plugin installed.

## To Set up an Environment for Google Cloud Platform
1. Open the **project.cfg** file, and add `gcp` to the list of environments.
```ini
[core]
environments = ['local', 'gcp']```

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