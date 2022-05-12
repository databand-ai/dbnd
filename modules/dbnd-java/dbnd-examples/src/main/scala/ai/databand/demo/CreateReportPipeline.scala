package ai.databand.demo

import java.security.SecureRandom

import ai.databand.annotations.Task
import ai.databand.log.DbndLogger
import ai.databand.spark.DbndSparkListener
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

object CreateReportPipeline {

    private val LOG = LoggerFactory.getLogger(classOf[ProcessDataSpark])

    @Task("create_report")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("CreateReportSparkScala")
            .getOrCreate
        val listener = new DbndSparkListener
        spark.sparkContext.addSparkListener(listener)

        val inputFile = args(0)
        val outputFile = args(1)

        val clean = spark.sqlContext
            .read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", ",")
            .load(inputFile)
        val report = createReport(clean)
        report.write.mode("overwrite").csv(outputFile)
        LOG.info("Pipeline finished")
    }

    @Task
    protected def createReport(data: Dataset[Row]): Dataset[Row] = {
        DbndLogger.logMetric("number of columns", data.columns.length)
        val score = data.first.getInt(6)
        val rand = new SecureRandom()
        DbndLogger.logMetric("avg score", score + rand.nextInt(2) * (-2) * rand.nextInt(data.columns.length) + rand.nextInt(10))
        data
    }

}
