package ai.databand;

import ai.databand.log.HistogramRequest;
import ai.databand.log.LogDatasetRequest;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.TaskRun;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.Dataset;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * No-op run used when no tracking is available to avoid unnecessary exceptions and log pollution.
 */
public class NoopDbndRun implements DbndRun {

    @Override
    public void init(Method method, Object[] args) {
        // do nothing
    }

    @Override
    public void startTask(Method method, Object[] args) {
        // do nothing
    }

    @Override
    public void errorTask(Method method, Throwable error) {
        // do nothing
    }

    @Override
    public void completeTask(Method method, Object result) {
        // do nothing
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public void stopExternal() {
        // do nothing
    }

    @Override
    public void error(Throwable error) {
        // do nothing
    }

    @Override
    public void logMetric(String key, Object value) {
        // do nothing
    }

    @Override
    public void logDataframe(String key, Dataset<?> value, HistogramRequest withHistograms) {
        // do nothing
    }

    @Override
    public void logHistogram(Map<String, Object> histogram) {
        // do nothing
    }

    @Override
    public void logDatasetOperation(String path,
                                    DatasetOperationType type,
                                    DatasetOperationStatus status,
                                    String error,
                                    String valuePreview,
                                    List<Long> dataDimensions,
                                    Object dataSchema,
                                    Boolean withPartition,
                                    List<ColumnStats> columnStats,
                                    String operationSource) {
        // do nothing
    }

    @Override
    public void logDatasetOperation(String path,
                                    DatasetOperationType type,
                                    DatasetOperationStatus status,
                                    Dataset<?> data,
                                    Throwable error,
                                    LogDatasetRequest params,
                                    String operationSource) {
        // do nothing
    }

    @Override
    public void logMetrics(Map<String, Object> metrics) {
        // do nothing
    }

    @Override
    public void logMetrics(Map<String, Object> metrics, String source) {
        // do nothing
    }

    @Override
    public void saveLog(LoggingEvent event, String formattedEvent) {
        // do nothing
    }

    @Override
    public void saveSparkMetrics(SparkListenerStageCompleted event) {
        // do nothing
    }

    @Override
    public String getTaskName(Method method) {
        // dummy
        return method.getName();
    }

    @Override
    public void setDriverTask(TaskRun taskRun) {
        // do nothing
    }
}
