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


## Supporting Inline Spark Tasks on EMR
In order to support inline tasks, install the `dbnd` and `dbnd-spark` package on your EMR cluster nodes. See [Installing DBND on Spark Cluster](doc:installing-dbnd-on-spark-cluster) for more information.