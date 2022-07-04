package ai.databand.examples

import ai.databand.annotations.Task
import ai.databand.deequ.DbndMetricsRepository
import ai.databand.log.{DbndLogger, LogDatasetRequest}
import ai.databand.schema.DatasetOperationStatus.{NOK, OK}
import ai.databand.schema.DatasetOperationType.{READ, WRITE}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

object ScalaSparkPipeline {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    private var sql: SQLContext = _

    @Task("spark_scala_pipeline")
    def main(args: Array[String]): Unit = {
        LOG.info("Starting pipeline")
        val spark = SparkSession.builder
            .appName("DBND Spark Scala Pipeline")
            .config("spark.sql.shuffle.partitions", 1)
            .master("local[*]")
            .getOrCreate

        sql = spark.sqlContext
        val path = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("sample.json").getFile
        val tracks = loadTracks(path)
        // there should be exactly 600 tracks
        VerificationSuite()
            .onData(tracks)
            .addCheck(
                Check(CheckLevel.Error, "Tracks testing")
                    .hasSize(_ == 600)
                    .isComplete("tracks"))
            .useRepository(new DbndMetricsRepository(new InMemoryMetricsRepository))
            .saveOrAppendResult(ResultKey(System.currentTimeMillis(), Map("name" -> "tracks")))
            .run()

        val tracksByArtistResult = countTracksByArtist(tracks)
        val tracksByNameResult = countTracksByTrackName(tracks)
        val result: Array[String] = Array(
            tracksByArtistResult.first().get(0).toString,
            tracksByNameResult.first().get(0).toString
        )
        collectRows(result)
        spark.stop
        LOG.info("Pipeline finished")
    }

    @Task
    def loadTracks(path: String): DataFrame = {
        LOG.info("Loading tracks data from file {}", path)
        val data = sql.read.json(path)
        DbndLogger.logDatasetOperation(path, READ, OK, data, new LogDatasetRequest().withPreview().withSchema());
        val result = data.selectExpr("explode(recenttracks.track) as tracks")
        LOG.info("Tracks was loaded from file {}", path)
        result
    }

    @Task
    def countTracksByTrackName(tracks: Dataset[Row]): Dataset[Row] = {
        LOG.info("Counting top tracks")
        val result = tracks.groupBy("tracks.name").count.orderBy(col("count").desc)
        DbndLogger.logDataframe("data", result, true)
        DbndLogger.logMetric("job_start_time", Long.MaxValue);
        DbndLogger.logMetric("job_start_time_test", System.currentTimeMillis());
        ColumnProfilerRunner()
            .onData(result)
            .useRepository(new DbndMetricsRepository(new InMemoryMetricsRepository))
            .saveOrAppendResult(ResultKey(System.currentTimeMillis(), Map("name" -> "topTracks")))
            .run()
        val topTrack = result.first()
        DbndLogger.logMetric("top_track_name", topTrack.get(0))
        DbndLogger.logMetric("top_track_playcount", topTrack.get(1))
        LOG.info("Track: {} with playcount: {}", topTrack.get(0), topTrack.get(1))
        additionalMetricTask(result)
        LOG.info("Completed counting top tracks")
        result
    }

    def additionalMetricTask(tracks: Dataset[Row]): Unit = {
        DbndLogger.logMetric("additional_tracks_metric", tracks.columns.length)
    }

    @Task
    def countTracksByArtist(tracks: Dataset[Row]): Dataset[Row] = {
        LOG.info("Counting top artists")
        val result = tracks.groupBy("tracks.artist.name").count.orderBy(col("count").desc)
        VerificationSuite()
            .onData(result)
            .addCheck(
                Check(CheckLevel.Error, "Tracks testing")
                    .hasSize(_ == 36)
                    .isUnique("name")
                    .isPositive("count")
                    .isComplete("name")
                    .isComplete("count"))
            .useRepository(new DbndMetricsRepository())
            .saveOrAppendResult(ResultKey(System.currentTimeMillis(), Map("name" -> "result")))
            .run()
        DbndLogger.logMetric("top_artist_playcount", result.first.get(1))
        LOG.info("Completed counting top artists")
        result
    }

    @Task
    def collectRows(args: Array[String]): String = {
        args.mkString("|")
    }

}
