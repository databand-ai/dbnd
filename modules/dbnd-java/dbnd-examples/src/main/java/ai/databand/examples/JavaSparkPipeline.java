package ai.databand.examples;

import ai.databand.annotations.Task;
import ai.databand.deequ.DbndMetricsRepository;
import ai.databand.deequ.DbndResultKey;
import ai.databand.log.DbndLogger;
import com.amazon.deequ.VerificationSuite;
import com.amazon.deequ.checks.Check;
import com.amazon.deequ.checks.CheckLevel;
import com.amazon.deequ.constraints.Constraint;
import com.amazon.deequ.profiles.ColumnProfilerRunner;
import com.amazon.deequ.repository.ResultKey;
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Predef;
import scala.collection.JavaConverters;

import java.util.Collections;

public class JavaSparkPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(JavaSparkPipeline.class);
    private final SQLContext sql;

    public JavaSparkPipeline(SparkSession spark) {
        this.sql = spark.sqlContext();
    }

    @Task("java_spark_pipeline")
    public void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];

        Dataset<Row> rawData = sql.read().format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(inputFile);
        Dataset<Row> imputed = unitImputation(rawData, new String[]{"10"}, "10");
        Dataset<Row> clean = dedupRecords(imputed, new String[]{"name"});
        Dataset<Row> report = createReport(clean);
        report.write().mode(SaveMode.Overwrite).csv(outputFile);
        LOG.info("Pipeline finished");
    }

    @Task
    protected Dataset<Row> unitImputation(Dataset<?> rawData, String[] columnsToImpute, String value) {
        int count = Integer.parseInt(rawData.describe().first().getString(2));
        DbndLogger.logMetric("Replaced NaNs", rawData.count() - count);
        return rawData.na().fill(value, columnsToImpute);
    }

    @Task
    protected Dataset<Row> dedupRecords(Dataset<Row> data, String[] keyColumns) {
        LOG.info("Dedup Records");
        Dataset<Row> result = data.dropDuplicates(keyColumns);
        DbndMetricsRepository metricsRepo = new DbndMetricsRepository(new InMemoryMetricsRepository());

        new VerificationSuite()
            .onData(result)
            .addCheck(
                new Check(CheckLevel.Error(), "Dedup testing", scala.collection.JavaConverters.collectionAsScalaIterableConverter(Collections.<Constraint>emptyList()).asScala().toSeq())
                    .isUnique("name", Option.empty())
                    .isUnique("id", Option.empty())
                    .isComplete("name", Option.empty())
                    .isComplete("id", Option.empty())
                    .isPositive("score", Check.IsOne(), Option.empty()))
            .useRepository(metricsRepo)
            .saveOrAppendResult(
                new ResultKey(System.currentTimeMillis(), JavaConverters.mapAsScalaMapConverter(Collections.singletonMap("name", "dedupedData")).asScala().toMap(Predef.$conforms())))
            .run();
        new ColumnProfilerRunner()
            .onData(result)
            .useRepository(metricsRepo)
            .saveOrAppendResult(new DbndResultKey("dedupedData"))
            .run();
        return result;
    }

    @Task
    protected Dataset<Row> createReport(Dataset<Row> data) {
        LOG.info("Create Report");
        DbndLogger.logMetric("number of columns", data.columns().length);
        double score = data.agg(Collections.singletonMap("score", "avg")).first().getAs("avg(score)");
        DbndLogger.logMetric("Avg Score", score);

        return data;
    }

}
