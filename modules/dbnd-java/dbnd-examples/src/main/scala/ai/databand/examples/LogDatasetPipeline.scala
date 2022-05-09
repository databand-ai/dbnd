package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.log.{DbndLogger, LogDatasetRequest}
import ai.databand.schema.DatasetOperationStatus.OK
import ai.databand.schema.DatasetOperationType.READ
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

object LogDatasetPipeline {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    private var sql: SQLContext = _

    @Task("log_dataset_pipeline")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("Log Dataset Pipeline")
            .master("local[*]")
            .getOrCreate

        sql = spark.sqlContext
        val path = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("usa-education-budget.csv").getFile
        val data = sql.read.option("inferSchema", "true").option("header", "true").csv(path)
        DbndLogger.logDatasetOperation("s3://databand/samples/usa-education-budget.csv", READ, OK, data, new LogDatasetRequest().withPreview().withSchema());
        spark.stop
    }
}
