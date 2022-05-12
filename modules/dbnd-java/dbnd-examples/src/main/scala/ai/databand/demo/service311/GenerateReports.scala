package ai.databand.demo.service311

import ai.databand.annotations.Task
import ai.databand.log.DbndLogger
import ai.databand.schema.DatasetOperationType
import ai.databand.schema.DatasetOperationType.READ
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object GenerateReports {

  @Task
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("service311_get_daily_data")
      //.master("local[*]")
      .getOrCreate

    val path = GetDailyData.buildPath()
    val df: DataFrame = spark.read.option("header","true").csv(f"$path/daily_data")
    DbndLogger.logDatasetOperation(path, READ, df)
    potholesReport(df, path)
    boroughsAndAgenciesReport(df, path)
  }

  @Task
  def potholesReport(df: DataFrame, path: String): Unit = {
    val totalPotholes = df.where("complaint_type = 'Street Condition'")
      .where("descriptor = 'Pothole'")
      .count()

    DbndLogger.logMetric("potholes.count", totalPotholes)

    val outputPath = f"$path/potholes_report"

    val report = df.where("complaint_type = 'Street Condition'")
      .where("descriptor = 'Pothole'")
      .groupBy("street_name")
      .count()

    report.write.format("csv").option("header","true").mode(SaveMode.Overwrite).save(outputPath)
    DbndLogger.logDatasetOperation(outputPath, DatasetOperationType.WRITE, report)
  }

  @Task
  def boroughsAndAgenciesReport(df: DataFrame, path: String): Unit = {
    val boroughs = df.groupBy("borough").count()
    val agencies = df.groupBy("agency").count()

    val boroughsPath = f"$path/boroughs"
    val agenciesPath = f"$path/agencies"

    boroughs.write.format("csv").option("header","true").mode(SaveMode.Overwrite).save(boroughsPath)
    DbndLogger.logDatasetOperation(boroughsPath, DatasetOperationType.WRITE, boroughs)

    agencies.write.format("csv").option("header","true").mode(SaveMode.Overwrite).save(agenciesPath)
    DbndLogger.logDatasetOperation(agenciesPath, DatasetOperationType.WRITE, agencies)
  }
}
