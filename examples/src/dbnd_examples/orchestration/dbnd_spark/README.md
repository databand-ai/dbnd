#Run Spark task at Dataabricks
##Configuration
uncomment [databricks] at databand-core.cfg

1. define airflow connection, by default databricks_default

```bash
dbnd connections --add \
    --conn_id databricks_default \
    --conn_type databricks \
    --conn_host <YOUR DATABRICKS CLUSTER URI> \
    --conn_extra "{\"token\": \"<YOUR ACCESS TOCKEN>\"}"
```

Configure aws to use the right spark engine, in our case its a databricks. at databnd-core.cfg i.e.

```ini
[aws]
_type = aws
dbnd_local_root = ${DBND_HOME}/data/dbnd
spark_engine = databricks
docker_engine = aws_batch
```

#Run Scenario
Run basic training pipeline:

```bash
dbnd run dbnd_examples.dbnd_spark.word_count_inline.word_count_inline --text s3://databand-playground/demo/customer_b.csv --env aws
```
