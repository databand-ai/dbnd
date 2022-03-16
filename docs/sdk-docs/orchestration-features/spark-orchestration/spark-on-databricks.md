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




>📘 Getting Databricks Cluster Id
> * API: https://<CLUSTER_IP>/2.0/clusters/list
> * UI: Under clusters -> advanced options-> tags -> ClusterId

### Configuring Databricks Cluster
 
You can configure DBND to spin up a new cluster for every job or use an existing cluster (default behavior). You will need to install and configure the DBND package on the cluster. See  [Installing DBND on Databricks Spark Cluster](doc:tracking-databricks) for more information