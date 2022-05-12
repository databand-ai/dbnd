package ai.databand.demo;

import ai.databand.annotations.Task;
import ai.databand.log.DbndLogger;
import ai.databand.spark.DbndSparkListener;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;

public class CreateReport {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessDataSpark.class);
    private final SQLContext sql;

    public CreateReport(SparkSession spark) {
        this.sql = spark.sqlContext();
    }

    @Task("create_report")
    public void createReport(String inputFile, String outputFile) {
        Dataset<Row> clean = sql.read().format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(inputFile);
        Dataset<Row> report = createReport(clean);
        report.write().csv(outputFile);
        LOG.info("Pipeline finished");
    }

    @Task
    protected Dataset<Row> createReport(Dataset<Row> data) {
        DbndLogger.logMetric("number of columns", data.columns().length);
        int score = data.first().getInt(6);
        SecureRandom rand = new SecureRandom();
        DbndLogger.logMetric("avg score", score + rand.nextInt(2) * (-2) * rand.nextInt(data.columns().length) + rand.nextInt(10));
        return data;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("CreateReportSparkJava")
            .getOrCreate();

        DbndSparkListener listener = new DbndSparkListener();
        spark.sparkContext().addSparkListener(listener);

        CreateReport processor = new CreateReport(spark);
        processor.createReport(args[0], args[1]);
    }

}
