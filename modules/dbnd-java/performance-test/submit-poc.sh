#!/usr/bin/env bash

cd .. && ./gradlew fatJar
cd performance-test || exit

export DBND__TRACKING=True
export DBND__CORE__DATABAND_URL=http://localhost:8080
export DBND__CORE__TRACKER_API=async-web
export DBND__ENABLE__SPARK_CONTEXT_ENV=True

spark-submit --conf "spark.driver.extraJavaOptions=-javaagent:../dbnd-agent/build/libs/dbnd-agent-latest-all.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \
             --conf "spark.sql.shuffle.partitions=1" \
             --conf "spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener" \
             benchmark-pyspark.py
