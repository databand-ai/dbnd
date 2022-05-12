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

public class ImputationAndDedup {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessDataSpark.class);
    private final SQLContext sql;

    public ImputationAndDedup(SparkSession spark) {
        this.sql = spark.sqlContext();
    }

    @Task("impute_and_dedup")
    public void imputeAndDedup(String inputFile, String outputFile) {
        String[] keyColumns = {"name"};
        String[] columnsToImpute = {"10"};

        Dataset<Row> rawData = sql.read().format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(inputFile);
        Dataset<Row> imputed = unitImputation(rawData, columnsToImpute, 10);
        Dataset<Row> clean = dedupRecords(imputed, keyColumns);
        clean.write().csv(outputFile);
        LOG.info("Pipeline finished");
    }

    @Task
    protected Dataset<Row> unitImputation(Dataset<Row> rawData, String[] columnsToImpute, int value) {
        int counter = Integer.parseInt(rawData.describe().first().getAs("phone"));
        SecureRandom rand = new SecureRandom();
        int noise = (-1) * rand.nextInt(2) * rand.nextInt(counter) + rand.nextInt(20);
        DbndLogger.logMetric("Replaced NaNs", counter + noise);
        return rawData.na().fill(value, columnsToImpute);
    }

    @Task
    protected Dataset<Row> dedupRecords(Dataset<Row> data, String[] keyColumns) {
        LOG.info("Dedup Records");
        data = data.dropDuplicates(keyColumns);
        DbndLogger.logMetric("data", data);
        return data;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("ImputeAndDedupSparkJava")
            .getOrCreate();

        DbndSparkListener listener = new DbndSparkListener();
        spark.sparkContext().addSparkListener(listener);

        ImputationAndDedup processor = new ImputationAndDedup(spark);
        processor.imputeAndDedup(args[0], args[1]);
    }

}
