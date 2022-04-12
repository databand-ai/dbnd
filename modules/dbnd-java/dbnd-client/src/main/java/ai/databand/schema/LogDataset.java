package ai.databand.schema;

import java.util.List;

public class LogDataset {

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

    public LogDataset(TaskRun taskRun,
                      String operationPath,
                      DatasetOperationType operationType,
                      DatasetOperationStatus operationStatus,
                      String operationError,
                      String valuePreview,
                      List<Long> dataDimensions,
                      Object dataSchema,
                      Boolean withPartition) {
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
            withPartition
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
                      Boolean withPartition) {
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

    @Override
    public String toString() {
        if (operationPath == null || operationType == null) {
            return "{empty operation}";
        }
        return String.format("{path: [%s], type: [%s], status: [%s]}", operationPath, operationType, operationStatus);
    }
}
