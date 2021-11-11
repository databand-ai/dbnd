package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.deequ.{DbndMetricsRepository, DbndResultKey}
import ai.databand.log.DbndLogger
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.slf4j.LoggerFactory

object Showcase {

    private val LOG = LoggerFactory.getLogger(Showcase.getClass)

    /**
     * @Task annotation marks method as a part of pipeline
     *
     * @param args
     */
    @Task("showcase")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("showcase")
            .master("local[*]")
            .getOrCreate
        val sql = spark.sqlContext
        val filePath = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("data.csv").getFile
        run(sql, filePath)
        spark.stop
    }

    @Task
    def run(sql: SQLContext, inputFile: String): Unit = {
        val rawData = sql.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", ",")
            .load(inputFile)
        // log any metrics using DbndLogger
        DbndLogger.logMetric("data_size", rawData.count)
        checkDataDeequ(rawData)
        profileDataDbnd(rawData)
        profileDataDeequ(rawData)
        LOG.info("Pipeline finished")
    }

    @Task
    protected def checkDataDeequ(data: Dataset[Row]): Dataset[Row] = {
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        // capture data verification result using Deequ and DbndMetricsRepository
        VerificationSuite()
            .onData(data)
            .addCheck(
                Check(CheckLevel.Error, "Data testing")
                    .isUnique("name")
                    .isUnique("id")
                    .isComplete("name")
                    .isComplete("id")
                    .isPositive("score"))
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("checkData"))
            .run()
        data
    }

    @Task
    protected def profileDataDeequ(data: Dataset[Row]): Dataset[Row] = {
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        // capture data profiling result and histograms using Deequ and DbndMetricsRepository
        ColumnProfilerRunner()
            .onData(data)
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("profileData"))
            .run()
        data
    }

    @Task
    protected def profileDataDbnd(data: Dataset[Row]): Dataset[Row] = {
        // log dataframe descriptive statistics and histogram using DbndLogger
        DbndLogger.logDataframe("data", data, true)
        data
    }

}
