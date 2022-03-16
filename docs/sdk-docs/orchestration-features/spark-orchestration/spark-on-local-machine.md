---
"title": "Spark on Local Machine"
---
The task described below will be run locally,  please follow these steps to make sure your local Spark is available: 

* [Install Spark](https://spark.apache.org/downloads.html)
* Install PySpark `pip install pyspark` 


## Configuring Local Spark

Set `spark_engine` to `local_spark`

Define connection:
```shell
$ airflow connections --delete --conn_id spark_default
$ airflow connections --add \
    --conn_id spark_default \
    --conn_type docker \
    --conn_host local \
    --conn_extra "{\"master\":\"local\",\"spark-home\":\"$(find_spark_home.py)\"}"
```