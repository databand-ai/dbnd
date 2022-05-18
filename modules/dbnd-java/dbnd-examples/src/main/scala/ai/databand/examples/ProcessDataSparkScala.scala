package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.deequ.{DbndMetricsRepository, DbndResultKey}
import ai.databand.log.DbndLogger
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import java.security.SecureRandom

object ProcessDataSparkScala {

    private val LOG = LoggerFactory.getLogger(ProcessDataSparkScala.getClass)
    private val random: SecureRandom = new SecureRandom()

    @Task("process_data_scala")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("CreateReportSparkScala")
            .getOrCreate
        val sql = spark.sqlContext

        val inputFile = args(0)
        val outputFile = args(1)

        val keyColumns = Array("name")
        val columnsToImpute = Array("10")
        val rawData = sql.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(inputFile)
        val imputed = unitImputation(rawData, columnsToImpute, 10)
        val clean = dedupRecords(imputed, keyColumns)
        val report = createReport(clean)
        report.write.csv(outputFile + "/" + System.currentTimeMillis())
        LOG.info("Pipeline finished")
    }

    @Task
    protected def unitImputation(rawData: DataFrame, columnsToImpute: Array[String], value: Int): DataFrame = {
        val counter = rawData.describe().first().getString(5).toInt
        val noise = (if (random.nextBoolean) 1 else -1) * random.nextInt(counter) + random.nextInt(20)
        DbndLogger.logMetric("Replaced NaNs", counter + noise)
        DbndLogger.logMetric("time", System.currentTimeMillis())
        rawData.na.fill(value, columnsToImpute)
    }

    @Task
    protected def dedupRecords(data: Dataset[Row], keyColumns: Array[String]): DataFrame = {
        LOG.info("Dedup Records")
        val result = data.dropDuplicates(keyColumns)
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        VerificationSuite()
            .onData(result)
            .addCheck(
                Check(CheckLevel.Error, "Dedup testing")
                    .isUnique("name")
                    .isUnique("id")
                    .isComplete("name")
                    .isComplete("id")
                    .isPositive("score"))
            .useRepository(metricsRepo)
            .saveOrAppendResult(ResultKey(System.currentTimeMillis(), Map("name" -> "dedupedData")))
            .run()
        DbndLogger.logDataframe("data", result, true)
        ColumnProfilerRunner()
            .onData(result)
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("dedupedData"))
            .run()
        analyzeDeduped(result)
        result
    }

    protected def analyzeDeduped(data: Dataset[Row]) = {
        DbndLogger.logMetric("dedupedAnalyzed", 1)
    }

    @Task
    protected def createReport(data: Dataset[Row]): Dataset[Row] = {
        LOG.info("Create Report")
        DbndLogger.logMetric("number of columns", data.columns.length)
        val score = data.first.getAs[Int]("score")
        DbndLogger.logMetric("Avg Score", score + (if (random.nextBoolean) 2 else -2) * random.nextInt(data.columns.length) + random.nextInt(10))

        data
    }

}
