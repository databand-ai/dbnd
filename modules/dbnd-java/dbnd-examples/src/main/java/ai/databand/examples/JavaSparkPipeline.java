package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import ai.databand.schema.DatasetOperationType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class JavaSparkPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(JavaSparkPipeline.class);
    private final SQLContext sql;

    public JavaSparkPipeline(SparkSession spark) {
        this.sql = spark.sqlContext();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("DBND Spark Java Pipeline")
            .master("local[*]")
            .getOrCreate();

        JavaSparkPipeline pipeline = new JavaSparkPipeline(spark);
        String path = args.length > 0 ? args[0] : JavaSparkPipeline.class.getClassLoader().getResource("sample.json").getFile();
        pipeline.execute(path);
    }

    @Task("spark_java_bad_pipeline")
    public Map<Dataset<Row>, Dataset<Row>> executeBad(String arg) {
        LOG.info("Starting pipeline with arg {}", arg);
        String path = getClass().getClassLoader().getResource("sample.json").getFile();
        Dataset<Row> tracks = loadTracks(path);
        Dataset<Row> tracksByArtist = countTracksByArtist(tracks);
        if ("error".equals(arg)) {
            LOG.error("Unable to complete the pipeline: error");
            throw new RuntimeException("Unable to complete the pipeline");
        }
        // calculate top tracks
        Dataset<Row> tracksByName = countTracksByTrackName(tracks);
        LOG.info("Pipeline finished");
        collectRows(new String[]{tracksByArtist.first().get(0).toString(), tracksByName.first().get(0).toString()});
        try {
            totalPlaycount(tracks);
        } catch (Exception e) {
            LOG.error("Unable to calculate total playcount", e);
        }
        return Collections.singletonMap(tracksByArtist, tracksByName);
    }

    @Task("spark_java_pipeline")
    public Map<Dataset<Row>, Dataset<Row>> execute(String path) {
        LOG.info("Starting pipeline with arg {}", path);
        Dataset<Row> tracks = loadTracks(path);
        Dataset<Row> tracksByArtist = countTracksByArtist(tracks);
        // calculate top tracks
        Dataset<Row> tracksByName = countTracksByTrackName(tracks);
        LOG.info("Pipeline finished");
        collectRows(new String[]{tracksByArtist.first().get(0).toString(), tracksByName.first().get(0).toString()});
        try {
            totalPlaycount(tracks);
        } catch (Exception e) {
            LOG.error("Unable to calculate total playcount", e);
        }
        LOG.info("Pipeline finished");
        return Collections.singletonMap(tracksByArtist, tracksByName);
    }

    @Task
    protected Dataset<Row> loadTracks(String path) {
        LOG.info("Loading tracks data from file {}", path);
        Dataset<Row> data = sql.read().json(path);
        Dataset<Row> result = data.selectExpr("explode(recenttracks.track) as tracks");
        LOG.info("Tracks was loaded from file {}", path);
        return result;
    }

    @Task
    protected Dataset<Row> countTracksByTrackName(Dataset<Row> tracks) {
        LOG.info("Counting top tracks");
        Dataset<Row> result = tracks.groupBy("tracks.name").count().orderBy(functions.col("count").desc());
        DbndLogger.logDataframe("data", result, true);
        DbndLogger.logDatasetOperation("file:///reports/tracks.csv", DatasetOperationType.WRITE, result);
        Row first = result.first();
        DbndLogger.logMetric("top_track_name", first.get(0));
        DbndLogger.logMetric("top_track_playcount", first.get(1));
        LOG.info("Track: {} with playcount: {}", first.get(0), first.get(1));
        LOG.info("Completed counting top tracks");
        return result;
    }

    @Task
    protected Dataset<Row> countTracksByArtist(Dataset<Row> tracks) {
        LOG.info("Counting top artists");
        Dataset<Row> result = tracks.groupBy("tracks.artist.name").count().orderBy(functions.col("count").desc());
        DbndLogger.logMetric("top_artist_playcount", result.first().get(1));
        LOG.info("Completed counting top artists");
        return result;
    }

    @Task
    protected String collectRows(String[] args) {
        return String.join("|", args);
    }

    @Task
    protected String totalPlaycount(Dataset<Row> tracks) {
        LOG.info("Counting total tracks playcount");
        throw new RuntimeException("Unable to count stuff");
    }
}
