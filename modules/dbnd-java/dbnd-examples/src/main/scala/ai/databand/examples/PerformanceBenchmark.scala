package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.deequ.{DbndMetricsRepository, DbndResultKey}
import ai.databand.log.{DbndLogger, HistogramRequest}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import java.io.FileWriter
import java.nio.file.{Files, Paths}

object PerformanceBenchmark {
    private val LOG = LoggerFactory.getLogger(PerformanceBenchmark.getClass)

    @Task("performance_benchmark")
    def main(args: Array[String]): Unit = {
        val inputFile = args(0)
        val runMethod = args(1)
        val runOption = args(2)

        val spark = SparkSession.builder
            .appName("performance-benchmark")
            .config("spark.driver.memory", "1024M")
            .master("local[*]")
            .getOrCreate

        val startTimestamp: Long = System.currentTimeMillis / 1000
        run(spark.sqlContext, inputFile, runMethod, runOption)
        val duration: Long = (System.currentTimeMillis / 1000 - startTimestamp)

        writeToFile(inputFile, startTimestamp.toString, duration.toString, runMethod, runOption)

        spark.stop()
    }

    def writeToFile(inputFile: String, startTs: String, duration: String, runMethod: String, runOption: String): Unit = {
        val isExist = Files.exists(Paths.get("./statistics.tsv"))
        val fw = new FileWriter("statistics.tsv", true)
        try {
            val columns = Array("InputFile", "StartTimestamp", "Duration", "Method", "Option")
            val records = Array(inputFile, startTs, duration, runMethod, runOption)

            if (!isExist) {
                fw.write(columns.mkString("\t") + "\n")
            }
            fw.write(records.mkString("\t") + "\n")
        } finally {
            fw.close()
        }
    }

    @Task("impute_and_dedup")
    def run(sql: SQLContext, inputFile: String, runMethod: String, runOption: String): Unit = {
        val rawData = sql.read.format("parquet").load(inputFile)
        //Add param to configure which one of 3 functions will run. This way we can measure time
        if (runMethod == "checkDataDeequ") {
            checkDataDeequ(rawData)
        }
        if (runMethod == "profileDataDbnd") {
            if (runOption == "includeAllNumericOnly") {
                var histogramRequest = new HistogramRequest(false).includeAllNumeric()
                profileDataDbnd(rawData, histogramRequest)
            } else {
                var histogramRequest = new HistogramRequest()
                profileDataDbnd(rawData, histogramRequest)
            }
        }
        if (runMethod == "profileDataDeequ") {
            profileDataDeeque(rawData)
        }

        LOG.info("Pipeline finished")
    }

    @Task
    protected def checkDataDeequ(data: Dataset[Row]): Dataset[Row] = {
        val metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository)
        val columns = data.columns.toSeq
        var check = Check(CheckLevel.Error, "Dedup testing")

        LOG.info("Run checks for columns: " + columns)

        for (column <- columns)
            check = check
                .isComplete(column)
                .isUnique(column)

        val verificationResult = VerificationSuite()
            .onData(data)
            .addCheck(check)
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
    protected def profileDataDbnd(data: Dataset[Row], histogramRequest: HistogramRequest): Dataset[Row] = {
        DbndLogger.logDataframe("data", data, histogramRequest)
        data
    }
}
