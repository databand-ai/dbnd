---
"title": "Installing JVM SDK and Agent"
---
Databand provides a set of Java libraries for tracking JVM-specific applications such as Spark jobs written in Scala or Java. Follow this guide to start tracking JVM applications.

# DBND JVM SDK.

## Adding Databand libraries to your JVM application
Please add the DBND library to your spark project, you should include DBND libraries in your FatJar or any other way you deploy your JVM project and its third-party dependencies to the spark cluster.

### Maven 
You should include DBND JVM SDK in your Maven project by adding it as a dependency in your POM file. 
``` xml
  <dependency>
      <groupId>ai.databand</groupId>
      <artifactId>dbnd-client</artifactId>
      <version>0.xx.x</version>
    </dependency>
```

### SBT
You should include DBND JVM SDK in your SBT project by adding the following line to your `build.sbt` file:
``` scala
   libraryDependencies += "ai.databand" % "dbnd-client" % "0.xx.x"
```

### Gradle
You should include DBND JVM SDK in your SBT project by adding the following line to your dependencies list at build.gradle file:
``` groovy
 compile('ai.databand:dbnd-client:0.xx.x')
```

### Manual
If you don't use a build system, or you just running PySpark script, and you still want to use Databand JVM binaries for [Listeners](doc:installing-jvm-dbnd#dbnd-listeners), you can download and add our JARs to the spark application manually via `--jars` or `--packages`.

You can use a direct link to Maven Repo. For production usage, it's highly recommended to pre-download JAR to local or remote storage.
Select the desired version of DBND and download `dbnd-agent-0.xx.x-all.jar` from [Maven Repository](https://repo1.maven.org/maven2/ai/databand/dbnd-agent/). For automation, you can use the following script. 
``` bash
DBND_VERSION=0.XX.X
wget https://repo1.maven.org/maven2/ai/databand/dbnd-agent/${DBND_VERSION}/dbnd-agent-${DBND_VERSION}-all.jar -P /home/hadoop/
```
You should store the agent JAR at the location available to your JVM application. In this example, we use `/home/hadoop/` folder, but you can use any other folder (your own user folder, if you run locally). For the usage inside your Spark Cluster, you can also publish this JAR to your remote storage (like Google Storage or S3 for example)


## Configuration.

Databand JVM SDK utilizes the same properties as Python SDK. However, not all of them are supported and ways of configuration are slightly different.

In general, JVM SDK is configured by passing environment variables to executable. In the case of Spark, variables can be set up by utilizing `spark.env` properties.
 
``` bash
# via export
export DBND__CORE__DATABAND_URL=...
# via spark.env
spark-submit ...  --conf "spark.env.DBND__TRACKING=True" ...
```

| Use `--conf` approach if you use distributed spark execution. Your environment variables from the current shell will not be copied to a remote machine.

Following configuration properties are supported in JVM SDK

| Variable | Default Value | Description |
|---|---|---|
| `DBND__TRACKING` | `False` | This property is mandatory.  This property explicitly enables tracking. Possible values: True/False. When not set or set to False, tracking won't be enabled. Should be explicitly set to True.  Note: when job is running inside Airflow, you can omit this property. |
| `DBND__CORE__DATABAND_URL` | Not set | This property is mandatory.  Tracker URL. |
| `DBND__CORE__DATABAND_ACCESS_TOKEN` | Not set | This property is mandatory.  Tracker [access token](doc:access-token). |
| `DBND__TRACKING__VERBOSE` | `False` | When set to True, enables verbose logging which can help with debugging agent instrumentation. |
| `DBND__TRACKING__LOG_VALUE_PREVIEW` | `False` | When set to True, previews for Spark datasets will be calculated. This can hit performance and should be explicitly enabled. |
| `DBND__LOG__PREVIEW_HEAD_BYTES` | `32768` | Size of the task log head in bytes. When log size exceeds head+tail, then middle of the log will be truncated |
| `DBND__LOG__PREVIEW_TAIL_BYTES` | `32768` | Size of the task log tail in bytes. When log size exceeds head+tail, then middle of the log will be truncated. |
| `DBND__RUN__JOB_NAME` | Spark Application name **or** main method name **or** [`@Task`](doc:jvm#tracking-pipeline-functions-with-annotations) annotation value if it was set | Allows to override job name. |
| `DBND__RUN__NAME` | Randomly generated string from [predefined list](https://github.com/databand-ai/dbnd/blob/develop/modules/dbnd-java/dbnd-client/src/main/java/ai/databand/RandomNames.java). | Allows to override run name. |


### Minimal Spark Configuration
The following environment variables should be defined in your Spark context/JVM Job. 
 * `DBND__CORE__DATABAND_URL` - a  Databand server URL  
 * `DBND__CORE__DATABAND_ACCESS_TOKEN` - a  Databand server Access Token  
 * `DBND__TRACKING=True` -enables JVM and Python in place tracking 


### Configure local Spark submit
| This is a "non-production" way of quickly trying and iterating around Databand Configuration at Spark Cluster.
An alternative approach is to add these variables to the environment variables available to your Spark Application. For `spark-submit` scripts, use `spark.env` for passing variables:
 
``` bash
spark-submit \
    --conf "spark.env.DBND__TRACKING=True" \ 
    --conf "spark.env.DBND__CORE__DATABAND_URL=REPLACE_WITH_DATABAND_URL" \
    --conf "spark.env.DBND__CORE__DATABAND_ACCESS_TOKEN=REPLACE_WITH_DATABAND_TOKEN"
```


### Airflow Tracking Context Properties
AIRFLOW_CONTEXT parameters are supported as a part of Airflow integration. These properties should be set for proper connection of JVM task run and parent Airflow task which triggered execution. See [Tracking Subprocess/Remote Tasks](doc:tracking-airflow-subprocess-remote) for more information

# DBND Listeners

## Setup Listener

You have to bring an extra package `ai.databand:dbnd-client` into the runtime of your spark application. You have the following options for doing that:
* In case you have your JVM project built and integrated with your spark environment, you can do that by changing your [JVM project config](doc:installing-jvm-dbnd#adding-databand-libraries-to-your-jvm-application). 
* Bring JAR directly to your spark application via [bootstrap](doc:installing-dbnd-on-spark-cluster#cluster-bootstrap) and add it to the `--jars` for you `spark-submit`. You can also use a direct link to Maven.
* Via spark `--packages` option:  `spark-submit --packages "ai.databand:dbnd-client:REPLACE_WITH_VERSION"`.
* With the Agent installed and enabled, you don't need to reference any specific DBND jar in your JVM project. Our agent jar already contains all relevant binaries.

## Enable Listener in your Application

You can enable our listener explicitly in the spark command line.
``` bash
spark-submit ... --conf 
 "spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener" 
```

# DBND JVM Agent
If you want to use JVM Agent, you'll have to manually integrate it into your Java application. Download it first to the location available to spark the process during the execution.  See [the instructions](doc:installing-jvm-dbnd#manual) above.

Your job has to be submitted with the following parameter:
``` bash
spark-submit ... --conf "spark.driver.extraJavaOptions=-javaagent:/opt/dbnd-agent-latest-all.jar
```

If you have an Agent you can enable Databand Listeners without explicitly referencing them in your JVM project. The agent will have all required DBND code in its FatJar (`-all.jar` file).