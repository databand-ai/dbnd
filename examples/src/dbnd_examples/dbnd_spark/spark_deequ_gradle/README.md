# Databand Spark/Deequ example

## Run using spark-submit

1. `./gradlew shadowJar`
2. ```spark-submit \
      --conf "spark.env.DBND__TRACKING=True" \
      --conf "spark.env.DBND__TRACKING__DATA_PREVIEW=True" \
      --conf "spark.env.DBND__CORE__DATABAND_URL=http://localhost:8080" \
      build/libs/spark_deequ_gradle-latest-all.jar src/main/resources/data.csv```

## Run using application distribution

1. `./gradlew installDist`
2. `DBND__TRACKING=True DBND__TRACKING__DATA_PREVIEW=True ./build/install/spark_deequ_gradle/bin/spark_deequ_gradle src/main/resources/data.csv`
