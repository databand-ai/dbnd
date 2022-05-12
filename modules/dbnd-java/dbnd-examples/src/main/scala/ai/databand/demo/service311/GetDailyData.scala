package ai.databand.demo.service311

import ai.databand.annotations.Task
import ai.databand.log.DbndLogger
import ai.databand.schema.DatasetOperationStatus.NOK
import ai.databand.schema.DatasetOperationType.{READ, WRITE}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, LocalDateTime}
import scala.io.Source

object GetDailyData {

  private val LOG: Logger = LoggerFactory.getLogger(GetDailyData.getClass)

  @Task
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("service311_get_daily_data")
      //.master("local[*]")
      .getOrCreate

    val url = buildUrl()

    val dfOpt: Option[DataFrame] = readData(spark, url)
    dfOpt match {
      case Some(df) => writeData(df)
      case None =>
    }
  }

  private val NYC_DATA_BASE_URL: String = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

  @Task
  def readData(spark: SparkSession, url: String): Option[DataFrame] = {
    try {
      val html = Source.fromURL(url)
      val jsonData = html.mkString
      DbndLogger.logMetric("311_api_response_size", jsonData.length)
      val rdd: RDD[String] = spark.sparkContext.makeRDD(jsonData :: Nil)
      val df: DataFrame = spark.read.json(spark.createDataset(rdd)(Encoders.STRING))
      DbndLogger.logDatasetOperation(NYC_DATA_BASE_URL, READ, df)
      Some(df)
    } catch {
      case e: Exception =>
        LOG.error("Unable to read data from NYC DB", e)
        DbndLogger.logDatasetOperation(NYC_DATA_BASE_URL, READ, NOK, spark.emptyDataFrame)
        Option.empty
    }
  }

  @Task
  def writeData(df: DataFrame): Unit = {
    if (df.schema.isEmpty) {
      LOG.error("Input data is empty")
      return
    }
    val path = f"${buildPath()}/daily_data"
    DbndLogger.logMetric("output_path", path)
    try {
      val columnsToDrop: Seq[String] = df.columns.filter(c => c.startsWith(":")).toSeq
      df.drop(columnsToDrop: _*).drop("location").write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(path)
      DbndLogger.logDatasetOperation(path, WRITE, df)
    } catch {
      case e: Exception =>
        LOG.error("Write to GCS was failed", e)
        DbndLogger.logDatasetOperation(path, WRITE, NOK, df)
    }
  }

  private val BUCKET = "dbnd-dev-playground-260010"
  private val FOLDER = "data/service311"

  def buildPath(): String = {
    val env: String = Option[String](System.getenv("AZKABAN_NAME_AND_NAMESPACE")).getOrElse("default")
    f"file:///demo/$FOLDER/$env/daily_data/date-${LocalDate.now().toString}"
  }

  def buildUrl(): String = {
    val endDate = LocalDateTime.now().withNano(0).withSecond(0).withMinute(0).withHour(0).minusDays(1)
    val startDate = endDate.minusDays(1)
    val filterRange = s"between '$startDate' and '$endDate'"
    DbndLogger.logMetric("filter_range", filterRange)
    val filter = s"(created_date $filterRange) or (closed_date $filterRange)"
    val filterEncoded = filter.replace(" ", "%20").replace("'", "%27").replace(":", "%3A")
    val url = s"$NYC_DATA_BASE_URL?$$limit=50000&$$where=$filterEncoded"
    url
  }
}
