package ai.databand.benchmarks

import ai.databand.annotations.Task
import ai.databand.deequ.{DbndMetricsRepository, DbndResultKey}
import ai.databand.log.DbndLogger
import ai.databand.spark.DbndSparkListener
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory


class RunExperiment(spark: SparkSession) {
    private val LOG = LoggerFactory.getLogger(classOf[RunExperiment])
    private val sqlContext = spark.sqlContext
    private val sparkContext = spark.sparkContext

    @Task("impute_and_dedup")
    def run(inputFile: String,
            outputFile: String,
            runDataCheck: Boolean,
            runDataDbndProfile: Boolean,
            runDataDeequProfile: Boolean): Unit = {
        val listener = new DbndSparkListener
        sparkContext.addSparkListener(listener)

        val rawData = sqlContext.read.format("parquet").load(inputFile)
        var flag = ""
        //Add param to configure which one of 3 functions will run. This way we can measure time
        if (runDataCheck) {
            checkDataDeequ(rawData)
            flag = "checkDataDeequ"
        }
        if (runDataDbndProfile) {
            profileDataDbnd(rawData)
            flag = "profileDataDbnd"
        }
        if (runDataDeequProfile) {
            profileDataDeeque(rawData)
            flag = "profileDataDeeque"
        }
        LOG.info("Pipeline finished [ " + flag + " ]")
    }

    @Task
    protected def checkDataDeequ(data: Dataset[Row]): Dataset[Row] = {
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        val verificationResult = VerificationSuite()
            .onData(data)
            .addCheck(
                Check(CheckLevel.Error, "Dedup testing")
                    .isUnique("boolean_0")
                    .isUnique("str_0")
                    .isUnique("num_0")
                    .isComplete("boolean_0")
                    .isComplete("str_0")
                    .isComplete("num_0"))
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("checkData"))
            .run()
        data
    }

    @Task
    protected def profileDataDeeque(data: Dataset[Row]): Dataset[Row] = {
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        ColumnProfilerRunner()
            .onData(data)
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("profileDataDeeque"))
            .run()
        data
    }

    @Task
    protected def profileDataDbnd(data: Dataset[Row]): Dataset[Row] = {
        DbndLogger.logDataframe("data", data, true)
        data
    }
}
