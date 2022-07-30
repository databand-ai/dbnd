/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import java.util.List;

public class LogDataset {

    public static final String OP_SOURCE_SPARK_QUERY_LISTENER = "spark_query_listener";
    public static final String OP_SOURCE_JAVA_MANUAL_LOGGING = "java_manual_logging";

    private final String runUid;
    private final String taskRunUid;
    private final String taskRunName;
    private final String taskRunAttemptUid;
    private final String operationPath;
    private final String operationType;
    private final String operationStatus;
    private final String operationError;
    private final String valuePreview;
    private final List<Long> dataDimensions;
    private final Object dataSchema;
    private final Boolean withPartition;
    private final List<ColumnStats> columnsStats;
    private final String operationSource;

    public LogDataset(TaskRun taskRun,
                      String operationPath,
                      DatasetOperationType operationType,
                      DatasetOperationStatus operationStatus,
                      String operationError,
                      String valuePreview,
                      List<Long> dataDimensions,
                      Object dataSchema,
                      Boolean withPartition,
                      List<ColumnStats> columnStats,
                      String operationSource) {
        this(
            taskRun.getRunUid(),
            taskRun.getTaskRunUid(),
            taskRun.getName(),
            taskRun.getTaskRunAttemptUid(),
            operationPath,
            operationType,
            operationStatus,
            operationError,
            valuePreview,
            dataDimensions,
            dataSchema,
            withPartition,
            columnStats,
            operationSource
        );
    }

    public LogDataset(String runUid,
                      String taskRunUid,
                      String taskRunName,
                      String taskRunAttemptUid,
                      String operationPath,
                      DatasetOperationType operationType,
                      DatasetOperationStatus operationStatus,
                      String operationError,
                      String valuePreview,
                      List<Long> dataDimensions,
                      Object dataSchema,
                      Boolean withPartition,
                      List<ColumnStats> columnStats,
                      String operationSource) {
        this.runUid = runUid;
        this.taskRunUid = taskRunUid;
        this.taskRunName = taskRunName;
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.operationPath = operationPath;
        this.operationType = operationType.toString();
        this.operationStatus = operationStatus.toString();
        this.operationError = operationError;
        this.valuePreview = valuePreview;
        this.dataDimensions = dataDimensions;
        this.dataSchema = dataSchema;
        this.withPartition = withPartition;
        this.columnsStats = columnStats;
        this.operationSource = operationSource;
    }

    public String getRunUid() {
        return runUid;
    }

    public String getTaskRunUid() {
        return taskRunUid;
    }

    public String getTaskRunName() {
        return taskRunName;
    }

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public String getOperationPath() {
        return operationPath;
    }

    public String getOperationType() {
        return operationType;
    }

    public String getOperationStatus() {
        return operationStatus;
    }

    public String getOperationError() {
        return operationError;
    }

    public String getValuePreview() {
        return valuePreview;
    }

    public List<Long> getDataDimensions() {
        return dataDimensions;
    }

    public Object getDataSchema() {
        return dataSchema;
    }

    public Boolean getWithPartition() {
        return withPartition;
    }

    public List<ColumnStats> getColumnsStats() {
        return columnsStats;
    }

    public String getOperationSource() {
        return operationSource;
    }

    @Override
    public String toString() {
        if (operationPath == null || operationType == null) {
            return "{empty operation}";
        }
        return String.format("{path: [%s], type: [%s], status: [%s]}", operationPath, operationType, operationStatus);
    }
}
