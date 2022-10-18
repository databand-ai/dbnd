---
"title": "Tracking Spark (Scala/Java)"
---
Databand provides a set of Java libraries for tracking JVM-specific applications such as Spark jobs written in Scala or Java. Follow this guide to start tracking JVM applications.


# Configuring SDK

Make sure you have followed [Installing JVM DBND](doc:installing-jvm-dbnd) guide to integrate DBND libraries into your Spark application. Use [Installing on Spark Cluster](doc:installing-dbnd-on-spark-cluster) to configure your  Spark Cluster.
 Following properties are required for proper tracking: `DBND__CORE__DATABAND_URL`, `DBND__CORE__DATABAND_ACCESS_TOKEN`, `DBND__TRACKING`.

# Tracking Job Metadata

The sections below describe different options available for tracking pipeline metadata.

## Logging Metrics

You can log any custom metrics that are important for pipeline and data observability. Examples include custom metrics for data quality information, like data counts or null counts, and custom KPIs particular to your data.

To enable logging of strings and numeric values, use the `ai.databand.log.DbndLogger.logMetric()` method:
`DbndLogger.logMetric("data", data);`

## Tracking Pipeline Functions With Annotations

If you have a more complex pipeline structure, or want to present your pipeline functions and store metadata as separate tasks, you can add annotations to your pipeline code. Method annotation will both enable input/output tracking for each method and link them visually.

To mark the methods that you want to track with the `@Task` annotation, use:
```scala
import ai.databand.annotations.Task

object ScalaSparkPipeline {
  @Task
  def main(args: Array[String]): Unit = {
    // init code
    // ...
    // task 1
	  val imputed = unitImputation(rawData, columnsToImpute, 10)
    // task 2
  	val clean = dedupRecords(imputed, keyColumns)
    // task 3
    val report = createReport(clean)
  }

  @Task
  protected def unitImputation(rawData: DataFrame, columnsToImpute: Array[String], value: Int): DataFrame = {
    // ...
  }

  @Task
  protected def dedupRecords(data: Dataset[Row], keyColumns: Array[String]): DataFrame = {
    // ...
  }

  @Task
  protected def createReport(data: Dataset[Row]): Dataset[Row] = {
    // ...
  }
}
```
```java
import ai.databand.annotations.Task;

public class ProcessDataSpark {

    @Task
    public void processCustomerData(String inputFile, String outputFile) {
        // setup code...
      	// task 1
        Dataset<Row> imputed = unitImputation(rawData, columnsToImpute, 10);
        // task 2
        Dataset<Row> clean = dedupRecords(imputed, keyColumns);
        // task 3
        Dataset<Row> report = createReport(clean);
        // ...
    }

    @Task
    protected Dataset<Row> unitImputation(Dataset<Row> rawData, String[] columnsToImpute, int value) {
        // ...
    }

    @Task
    protected Dataset<Row> dedupRecords(Dataset<Row> data, String[] keyColumns) {
        // ...
    }

    @Task
    protected Dataset<Row> createReport(Dataset<Row> data) {
        // ...
    }
}
```

To use annotations and track the flow of tasks with annotations, the Databand Java agent instruments your application and should be included in the application startup script. See [Installing JVM SDK and Agent](doc:installing-jvm-dbnd)  and [Installing DBND on Spark Cluster](doc:installing-dbnd-on-spark-cluster) .

Your job has been submitted with the following parameter:
``` bash
spark-submit ... --conf "spark.driver.extraJavaOptions=-javaagent:REPLACE_WITH_PATH_TO_AGENT/dbnd-agent-${DBND_VERSION}-all.jar
```


## Logging Dataset Operations

Databand allows you to track your dataset operations. You need to use `DbndLogger.logDatasetOperation()`:
```java
import ai.databand.log.DbndLogger;

//...

    @Task("create_report")
    public void ingestData(String path) {
        Dataset<Row> data = sql.read().json(path);
        // 1. Track simple:
        DbndLogger.logDatasetOperation(path, DatasetOperationType.READ, data);

        //2. Track passed/failed operation with error details:
        try {
          ...
          DbndLogger.logDatasetOperation(path, DatasetOperationType.READ, data);
        } catch {
          case e: Exception =>
            DbndLogger.logDatasetOperation(path, DatasetOperationType.READ, DatasetOperationStatus.NOK, data, e)
        }

        //3. Track failed operation:
        DbndLogger.logDatasetOperation(path, DatasetOperationType.READ, DatasetOperationStatus.NOK, data)

        // track without preview/schema:
        DbndLogger.logDatasetOperation(path, DatasetOperationType.READ, DatasetOperationStatus.OK, data, false, false);
    }

//...
```

For more details, see [Dataset Logging](doc:dataset-logging).

## Job Logging

Databand support logs limit and head/tail logging. Following properties  are responsible for controlling it:

* `DBND__LOG__PREVIEW_HEAD_BYTES` specifies how many bytes should be fetched from log head
* `DBND__LOG__PREVIEW_TAIL_BYTES` specifies how many bytes should be fetched from log tail


##Enabling Tracking Spark metrics and I/O

Databand can capture [Spark Executor metrics](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics), and any I/O operation by your spark code. Please check [Tracking Spark/JVM Applications](doc:tracking-spark-applications#automatic-tracking-of-spark-metrics-and-io-via-dbnd-listeners) for more information.

These listeners can be enabled via [configuration](doc:installing-jvm-dbnd) . We suggest you use this method.

For trying out this feature you can add it programmatically to your spark application. Add the Databand Spark Listener to your Spark context:
```scala
import ai.databand.annotations.Task
import ai.databand.spark.DbndSparkListener
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CreateReport {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("CreateReportSparkScala")
            .getOrCreate
        val listener = new DbndSparkListener
        spark.sparkContext.addSparkListener(listener)
    }

}
```
```java
import ai.databand.annotations.Task;
import ai.databand.spark.DbndSparkListener;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class CreateReport {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("CreateReportSparkJava")
            .getOrCreate();

        DbndSparkListener listener = new DbndSparkListener();
        spark.sparkContext().addSparkListener(listener);
				//...
    }

}
```



## Spark with Deequ for Data Quality Metrics
### Installation
A prerequisite for using Deequ is adding [`deequ`](https://github.com/awslabs/deequ) JARs and `ai.databand:dbnd-api-deequ` to your project dependencies:
```SBT
libraryDependencies ++= Seq(
  "com.amazon.deequ" % "deequ" % "x.x.x-spark-x.x"
  "ai.databand" % "dbnd-api-deequ" % "0.xx.x",
)
```
```Maven
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.amazon.deequ</groupId>
      <artifactId>deequ</artifactId>
      <version>x.x.x-spark-x.x</version>
    </dependency>
    <dependency>
      <groupId>ai.databand</groupId>
      <artifactId>dbnd-api-deequ</artifactId>
      <version>0.xx.x</version>
    </dependency>
  </dependencies>
</dependencyManagement>
```
```gradle
dependencies {
  implementation 'com.amazon.deequ:deequ:x.x.x-spark-x.x'
  implementation 'ai.databand:dbnd-api-deequ:0.xx.x'
}
```

### DBND JVM Deequ Metrics Repository
Databand utilizes a custom MetricsRepository and DbndResultKey. You need to explicitly add both to the code:
```scala
import ai.databand.deequ.DbndMetricsRepository

@Task
protected def dedupRecords(data: Dataset[Row], keyColumns: Array[String]): Dataset[Row] = {
    val dedupedData = data.dropDuplicates(keyColumns)
    // custom metrics repository
    val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
    // capturing dataset verification results
    VerificationSuite()
        .onData(dedupedData)
        .addCheck(
            Check(CheckLevel.Error, "Dedup testing")
                .isUnique("name")
                .isUnique("id")
                .isComplete("name")
                .isComplete("id")
                .isPositive("score"))
        .useRepository(metricsRepo)
        .saveOrAppendResult(new DbndResultKey("dedupedData"))
        .run()
    // using metrics repositoty to capture dataset profiling results
    ColumnProfilerRunner()
        .onData(dedupedData)
        .useRepository(metricsRepo)
        .saveOrAppendResult(new DbndResultKey("dedupedData"))
        .run()
}

```

If you already use a metrics repository, you can wrap it inside Databand's `new DbndMetricsRepository(new InMemoryMetricsRepository)`. Databand will first submit the metrics to the wrapped repository and to the Databand tracker afterward.

To distinguish metric keys, you should use a special `DbndResultKey`. We recommend giving your checks/profiles names that will allow you to clearly distinguish them in the Databand's monitoring UI.

### A Note on Scala/Spark Compatibility
Databand library is Scala/Spark-agnostic and can be used with any combination of Scala/Spark. However, the Deequ version should be selected carefully to match your needs. Please refer to [Deequ docs](https://github.com/awslabs/deequ) and select the exact version from the list of [available versions](https://repo1.maven.org/maven2/com/amazon/deequ/deequ/).

[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>"
}
[/block]
