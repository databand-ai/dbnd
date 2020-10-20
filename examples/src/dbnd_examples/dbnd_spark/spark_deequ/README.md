# Databand Spark/Deequ example

## Run using spark-submit

0. `wget https://dbnd-dev-maven-repository.s3.us-east-2.amazonaws.com/ai/databand/dbnd-agent/0.29.0/dbnd-agent-0.29.0-all.jar`
1. `./gradlew shadowJar`
2. ```spark-submit \
      --conf "spark.driver.extraJavaOptions=-javaagent:dbnd-agent-0.29.0-all.jar" \
      --conf "spark.env.DBND__TRACKING=True" \
      --conf "spark.env.DBND__TRACKING__DATA_PREVIEW=True" \
      --conf "spark.env.DBND__CORE__DATABAND_URL=http://localhost:8080" \
      build/libs/spark_deequ-0.29.0-all.jar src/main/resources/data.csv```

## Run using application distribution

0. `wget https://dbnd-dev-maven-repository.s3.us-east-2.amazonaws.com/ai/databand/dbnd-agent/0.29.0/dbnd-agent-0.29.0-all.jar`
1. `./gradlew installDist`
2. `DBND__TRACKING=True DBND__TRACKING__DATA_PREVIEW=True JAVA_OPTS="-javaagent:dbnd-agent-0.29.0-all.jar" ./build/install/spark_deequ/bin/spark_deequ src/main/resources/data.csv`
