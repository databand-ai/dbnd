#!/usr/bin/env bash

cd .. && ./gradlew fatJar
cd performance-test || exit

export DBND__TRACKING=True
export DBND__ENABLE__SPARK_CONTEXT_ENV=True

spark-submit --conf "spark.driver.extraJavaOptions=-javaagent:../dbnd-agent/build/libs/dbnd-agent-latest-all.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \
             --conf "spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener" \
             --conf "spark.env.DBND__TRACKING=True" \
             --conf "spark.env.DBND__RUN__JOB_NAME=kek-job" \
             benchmark-pyspark.py
