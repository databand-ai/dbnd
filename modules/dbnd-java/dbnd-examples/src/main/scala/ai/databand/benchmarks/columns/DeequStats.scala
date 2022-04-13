package ai.databand.benchmarks.columns

import com.amazon.deequ.profiles.{ColumnProfilerRunBuilder, ColumnProfiles, NumericColumnProfile, StandardColumnProfile}
import org.apache.spark.sql.SparkSession

object DeequStats {

    def main(args: Array[String]): Unit = {
        val dataPath = args(0)

        val spark = SparkSession.builder.appName("SparkStats")
            .master("spark://127.0.0.1:7077")
            .getOrCreate

        val df = spark.read
            .option("inferSchema", "True")
            .option("header", "True")
            .csv(dataPath)
            .select("capacity_bytes", "smart_1_raw", "smart_5_raw", "smart_9_raw", "smart_194_raw", "smart_197_raw")

        val result: ColumnProfiles = new ColumnProfilerRunBuilder(df).withLowCardinalityHistogramThreshold(1).run()

        result.profiles.foreach { case (name, profile) =>
            profile match {
                case n: NumericColumnProfile => {
                    println(s"Statistics of '$name':\n" +
                        s"\tcompleteness: ${n.completeness}\n" +
                        s"\tapprox distinct: ${n.approximateNumDistinctValues}\n" +
                        s"\tmin: ${n.minimum.get}\n" +
                        s"\tmax: ${n.maximum.get}\n" +
                        s"\tmean: ${n.mean.get}\n" +
                        s"\tsum: ${n.sum.get}\n" +
                        s"\tstddev: ${n.stdDev.get}\n")
                }
                case s: StandardColumnProfile => {
                    println(s"Statistics of '$name':\n" +
                        s"\tcompleteness: ${s.completeness}\n" +
                        s"\tapprox distinct: ${s.approximateNumDistinctValues}\n")
                }
            }
        }
    }

}
