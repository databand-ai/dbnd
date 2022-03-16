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