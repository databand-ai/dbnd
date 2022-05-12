package ai.databand.demo

import ai.databand.deequ.{DbndMetricsRepository, DbndResultKey}
import ai.databand.log.DbndLogger
import ai.databand.spark.DbndSparkListener
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.security.SecureRandom

object ImputeAndDedupPipelineSdkOnly {

    private val LOG = LoggerFactory.getLogger(classOf[ProcessDataSpark])

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("ImputeAndDedupSparkScalaSdkOnly")
            .getOrCreate
        val listener = new DbndSparkListener
        spark.sparkContext.addSparkListener(listener)

        val inputFile = args(0)
        val outputFile = args(1)

        val keyColumns = Array("name")
        val columnsToImpute = Array("10")
        val rawData = spark
            .sqlContext
            .read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", ",")
            .load(inputFile)
        val imputed = unitImputation(rawData, columnsToImpute, 10)
        val clean = dedupRecords(imputed, keyColumns)
        clean.write.mode("overwrite").csv(outputFile)
        LOG.info("Pipeline finished")
    }

    protected def unitImputation(rawData: Dataset[Row], columnsToImpute: Array[String], value: Int): Dataset[Row] = {
        val counter = Integer.parseInt(rawData.describe().first.getAs("phone"))
        val rand = new SecureRandom()
        val noise = (-1) * rand.nextInt(2) * rand.nextInt(counter) + rand.nextInt(20)
        DbndLogger.logMetric("Replaced NaNs", counter + noise)
        rawData.na.fill(value, columnsToImpute)
    }

    protected def dedupRecords(data: Dataset[Row], keyColumns: Array[String]): Dataset[Row] = {
        LOG.info("Dedup Records")
        val dedupedData = data.dropDuplicates(keyColumns)
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        val verificationResult = VerificationSuite()
            .onData(dedupedData)
            .addCheck(
                Check(CheckLevel.Error, "Dedup testing")
                    .isUnique("name")
                    .isUnique("id")
                    .isComplete("name")
                    .isComplete("id")
                    .isPositive("score"))
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey(System.currentTimeMillis(), Map("name" -> "dedupedData"), "dedupedData"))
            .run()
        DbndLogger.logMetric("deeque.dedup.verificationResult", verificationResult.status)
        DbndLogger.logMetric("data", dedupedData)
        DbndLogger.logDataframe("data", dedupedData, true)
        ColumnProfilerRunner()
            .onData(dedupedData)
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("dedupedData"))
            .run()
        dedupedData
    }

}
