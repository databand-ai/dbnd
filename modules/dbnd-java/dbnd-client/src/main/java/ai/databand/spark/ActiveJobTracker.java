package ai.databand.spark;

import ai.databand.DbndWrapper;
import org.apache.spark.Dependency;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import scala.collection.Iterator;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

public class ActiveJobTracker {

    /**
     * Extract IO information from Spark ActiveJob.
     *
     * @param job
     */
    public static void track(ActiveJob job) {
        Stage stage = job.finalStage();
        List<RDD<?>> all = allRdds(stage.rdd());

        List<SparkIOSource> sources = new ArrayList<>(1);

        for (RDD<?> rdd : all) {
            if (rdd instanceof FileScanRDD) {
                FileScanRDD fs = (FileScanRDD) rdd;
                Iterator<FilePartition> iterator = fs.filePartitions().iterator();
                while (iterator.hasNext()) {
                    FilePartition filePart = iterator.next();
                    for (PartitionedFile file : filePart.files()) {
                        Map<String, Object> properties = new HashMap<>(1);
                        properties.put("start", file.start());
                        properties.put("length", file.length());
                        properties.put("index", filePart.index());
                        sources.add(new SparkIOSource(file.filePath(), "file_scan_rdd", properties));
                    }
                }
            } else if (rdd instanceof JDBCRDD) {
                JDBCRDD db = (JDBCRDD) rdd;
                Optional<SparkIOSource> dbSource = extractDataFromJdbcRdd(db);
                dbSource.ifPresent(sources::add);
            }
        }

        // TODO: Track Snowflake RDD

        Map<String, Object> metrics = sources.stream().collect(Collectors.toMap(SparkIOSource::metricKey, io -> io));
        DbndWrapper.instance().logMetrics(metrics, "spark");
    }

    public static Optional<SparkIOSource> extractDataFromJdbcRdd(JDBCRDD rdd) {
        try {
            Field optionsField = rdd.getClass().getDeclaredField("options");
            optionsField.setAccessible(true);
            Field columnListField = rdd.getClass().getDeclaredField("columnList");
            columnListField.setAccessible(true);

            JDBCOptions options = (JDBCOptions) optionsField.get(rdd);
            String columnsList = (String) columnListField.get(rdd);

            Map<String, Object> properties = new HashMap<>(2);
            Map<String, Object> jdbcOptions = new HashMap<>(2);
            jdbcOptions.put("url", options.url());
            jdbcOptions.put("table_or_query", options.tableOrQuery());
            properties.put("options", jdbcOptions);
            properties.put("columnsList", columnsList);

            return Optional.of(new SparkIOSource(options.url(), "jdbc_rdd", properties));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return Optional.empty();
        }

    }

    /**
     * Recursively extract all rdds from job.
     *
     * @param rdd
     * @return
     */
    public static List<RDD<?>> allRdds(RDD<?> rdd) {
        List<RDD<?>> result = new ArrayList<>(1);
        Queue<RDD<?>> stack = new ArrayDeque<>(Collections.singletonList(rdd));
        while (!stack.isEmpty()) {
            RDD<?> polled = stack.poll();
            result.add(polled);
            for (scala.collection.Iterator<Dependency<?>> it = polled.dependencies().iterator(); it.hasNext(); ) {
                Dependency<?> next = it.next();
                stack.add(next.rdd());
            }
        }
        return result;
    }

}
