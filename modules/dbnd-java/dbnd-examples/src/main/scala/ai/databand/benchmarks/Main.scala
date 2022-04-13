package ai.databand.benchmarks

import org.apache.spark.sql.SparkSession

import java.io.FileWriter
import java.nio.file.{Files, Paths}

object Main {
    def main(args: Array[String]): Unit = {
        val appName = args(0)
        val inputFile = args(1)
        val outputFile = args(2)
        val runDataCheck = args(3).toBoolean
        val runDataDbndProfile = args(4).toBoolean
        val runDataDeequProfile = args(5).toBoolean

        val spark = SparkSession.builder.appName(appName).master("local[*]").getOrCreate
        val processor = new RunExperiment(spark)

        val startTimestamp: Long = System.currentTimeMillis / 1000
        processor.run(inputFile, outputFile, runDataCheck, runDataDbndProfile, runDataDeequProfile)
        val endTimestamp: Long = System.currentTimeMillis / 1000

        val isExist = Files.exists(Paths.get("./statistics.tsv"))
        val fw = new FileWriter("statistics.tsv", true)
        try {
            val durationStr = (endTimestamp - startTimestamp).toString
            val startTsStr = startTimestamp.toString

            val columns = Array(
                "AppName", "InputFile", "OutputFile", "StartTimestamp", "Duration",
                "RunDataCheck", "RunDataDbndProfile", "RunDataDeequeProfile"
            )
            val records = Array(
                appName, inputFile, outputFile, startTsStr, durationStr,
                runDataCheck, runDataDbndProfile, runDataDeequProfile
            )

            if (!isExist) {
                fw.write(columns.mkString("\t") + "\n")
            }
            fw.write(records.mkString("\t") + "\n")
        } finally {
            fw.close()
        }

        spark.stop()
    }
}
