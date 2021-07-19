package ai.databand;

import ai.databand.log.HistogramRequest;
import ai.databand.schema.DatasetOperationStatuses;
import ai.databand.schema.DatasetOperationTypes;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.Dataset;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * DBND run.
 */
public interface DbndRun {

    /**
     * Init run in DBND using pipeline root execution method pointcut.
     *
     * @param method
     * @param args
     */
    void init(Method method, Object[] args);

    /**
     * Start task in the run context.
     *
     * @param method
     * @param args
     */
    void startTask(Method method, Object[] args);

    /**
     * Set task state to 'error'.
     *
     * @param method
     * @param error
     */
    void errorTask(Method method, Throwable error);

    /**
     * Set task state to 'completed'.
     *
     * @param method
     * @param result
     */
    void completeTask(Method method, Object result);

    /**
     * Stop run. Set run state to 'completed'.
     */
    void stop();

    /**
     * Stop run. Set run state to 'failed'.
     *
     * @param error
     */
    void error(Throwable error);

    /**
     * Log metric and attach it to the current task.
     *
     * @param key
     * @param value
     */
    void logMetric(String key, Object value);

    /**
     * Log Spark dataframe
     *
     * @param key
     * @param value
     * @param histogramRequest
     */
    void logDataframe(String key, Dataset<?> value, HistogramRequest histogramRequest);

    /**
     * Log histogram object.
     *
     * @param histogram
     */
    void logHistogram(Map<String, Object> histogram);

    /**
     * Log dataset operations.
     *
     * @param operationPath   operation path
     * @param operationType   operation type (read/write)
     * @param operationStatus operation status (success/failed)
     * @param valuePreview    dataset preivew
     * @param dataDimensions  dataset dimenstions
     * @param dataSchema      dataset schema
     */
    void logDatasetOperation(String operationPath,
                             DatasetOperationTypes operationType,
                             DatasetOperationStatuses operationStatus,
                             String valuePreview,
                             List<Long> dataDimensions,
                             String dataSchema);

    /**
     * Log Deequ result
     *
     * @param dfName
     * @param analyzerContext
     */
//    void logDeequResult(String dfName, AnalyzerContext analyzerContext);

    /**
     * Log metrics batch and attach it to the current task.
     *
     * @param metrics
     */
    void logMetrics(Map<String, Object> metrics);

    /**
     * Log metrics batch with source
     *
     * @param metrics
     * @param source
     */
    void logMetrics(Map<String, Object> metrics, String source);

    /**
     * Save log and attach it to the current task and all parent tasks.
     *
     * @param event
     * @param formattedEvent
     */
    void saveLog(LoggingEvent event, String formattedEvent);

    /**
     * Save spark metrics.
     *
     * @param event
     */
    void saveSparkMetrics(SparkListenerStageCompleted event);

    /**
     * Extract task name either from method name or annotation value.
     *
     * @param method
     * @return task name extracted from method.
     */
    String getTaskName(Method method);
}
