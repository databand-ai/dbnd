package ai.databand.demo

import ai.databand.log.DbndLogger
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import java.security.SecureRandom

class ProcessDataSparkScalaSdkOnly(spark: SparkSession) {
    private val LOG = LoggerFactory.getLogger(classOf[ProcessDataSparkScalaSdkOnly])
    private val sql: SQLContext = spark.sqlContext
    private val random: SecureRandom = new SecureRandom()

    def processCustomerData(inputFile: String, outputFile: String): Unit = {
        val keyColumns = Array("name")
        val columnsToImpute = Array("10")
        val rawData = sql.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(inputFile)
        val imputed = unitImputation(rawData, columnsToImpute, 10)
        val clean = dedupRecords(imputed, keyColumns)
        val report = createReport(clean)
        report.write.csv(outputFile)
        LOG.info("Pipeline finished")
    }

    protected def unitImputation(rawData: DataFrame, columnsToImpute: Array[String], value: Int): DataFrame = {
        val counter = rawData.describe().first().getString(5).toInt
        val noise = (if (random.nextBoolean) 1 else -1) * random.nextInt(counter) + random.nextInt(20)
        DbndLogger.logMetric("Replaced NaNs", counter + noise)
        rawData.na.fill(value, columnsToImpute)
    }

    protected def dedupRecords(data: Dataset[Row], keyColumns: Array[String]): DataFrame = {
        LOG.info("Dedup Records")
        val result = data.dropDuplicates(keyColumns)
        DbndLogger.logMetric("data", result)
        result
    }

    protected def createReport(data: Dataset[Row]): Dataset[Row] = {
        LOG.info("Create Report")
        DbndLogger.logMetric("number of columns", data.columns.length)
        val score = data.first.getAs[Int]("score")
        DbndLogger.logMetric("Avg Score", score + (if (random.nextBoolean) 2 else -2) * random.nextInt(data.columns.length) + random.nextInt(10))

        data
    }

}
