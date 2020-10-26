# Databand Spark/Deequ SBT example

## Run using spark-submit

1. `sbt assembly`
2. ```spark-submit \
      --conf "spark.env.DBND__TRACKING=True" \
      --conf "spark.env.DBND__TRACKING__DATA_PREVIEW=True" \
      --conf "spark.env.DBND__CORE__DATABAND_URL=http://localhost:8080" \
      target/scala-2.12/spark_deequ_sbt-assembly-latest.jar src/main/resources/data.csv```

