#!/usr/bin/env bash

/spark/bin/spark-submit --conf "spark.driver.extraJavaOptions=-javaagent:/apps/dbnd-java/dbnd-agent/build/libs/dbnd-agent-latest-all.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \
                        --conf "spark.env.dbnd.tracking=True" \
                        --conf "spark.env.dbnd.core.databand_url=http://localhost:8080" \
                        --class ai.databand.examples.HiveLocalExample \
                        /apps/dbnd-java/dbnd-examples/build/libs/dbnd-examples-latest-all.jar
