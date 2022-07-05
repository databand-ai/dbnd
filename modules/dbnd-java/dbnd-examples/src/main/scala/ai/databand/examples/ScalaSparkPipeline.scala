package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.deequ.{DbndMetricsRepository, DbndResultKey}
import ai.databand.log.DbndLogger
import ai.databand.spark.DbndSparkQueryExecutionListener
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

object ScalaSparkPipeline {

    private val LOG = LoggerFactory.getLogger(ProcessDataSparkScala.getClass)

    @Task("scala_spark_pipeline")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("ScalaSparkPipeline")
            .config("spark.sql.shuffle.partitions", 1)
            .master("local[*]")
            .getOrCreate
        spark.listenerManager.register(new DbndSparkQueryExecutionListener())
        val sql = spark.sqlContext

        val inputFile = args(0)
        val outputFile = args(1)

        val rawData = sql.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(inputFile)
        val imputed = unitImputation(rawData, Array("10"), "10")
        val clean = dedupRecords(imputed, Array("name"))
        val report = createReport(clean)
        report.write.mode(SaveMode.Overwrite).csv(outputFile)
        spark.stop
        LOG.info("Pipeline finished")
    }

    @Task
    protected def unitImputation(rawData: DataFrame, columnsToImpute: Array[String], value: String): DataFrame = {
        val count = rawData.describe().first().getString(2).toInt
        DbndLogger.logMetric("Replaced NaNs", rawData.count() - count)
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
        ColumnProfilerRunner()
            .onData(result)
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("dedupedData"))
            .run()
        result
    }

    @Task
    protected def createReport(data: Dataset[Row]): Dataset[Row] = {
        LOG.info("Create Report")
        DbndLogger.logMetric("number of columns", data.columns.length)
        val score = data.agg(("score", "avg")).first.getAs[Double]("avg(score)")
        DbndLogger.logMetric("Avg Score", score)

        data
    }

}
