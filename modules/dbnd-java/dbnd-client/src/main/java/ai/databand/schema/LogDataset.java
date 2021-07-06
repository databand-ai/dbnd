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
    private final String valuePreview;
    private final List<Long> dataDimensions;
    private final String dataSchema;

    public LogDataset(TaskRun taskRun,
                      String operationPath,
                      DatasetOperationTypes operationType,
                      DatasetOperationStatuses operationStatus,
                      String valuePreview,
                      List<Long> dataDimensions,
                      String dataSchema) {
        this(
            taskRun.getRunUid(),
            taskRun.getTaskRunUid(),
            taskRun.getName(),
            taskRun.getTaskRunAttemptUid(),
            operationPath,
            operationType,
            operationStatus,
            valuePreview,
            dataDimensions,
            dataSchema
        );
    }

    public LogDataset(String runUid,
                      String taskRunUid,
                      String taskRunName,
                      String taskRunAttemptUid,
                      String operationPath,
                      DatasetOperationTypes operationType,
                      DatasetOperationStatuses operationStatus,
                      String valuePreview,
                      List<Long> dataDimensions,
                      String dataSchema) {
        this.runUid = runUid;
        this.taskRunUid = taskRunUid;
        this.taskRunName = taskRunName;
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.operationPath = operationPath;
        this.operationType = operationType.toString();
        this.operationStatus = operationStatus.toString();
        this.valuePreview = valuePreview;
        this.dataDimensions = dataDimensions;
        this.dataSchema = dataSchema;
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

    public String getValuePreview() {
        return valuePreview;
    }

    public List<Long> getDataDimensions() {
        return dataDimensions;
    }

    public String getDataSchema() {
        return dataSchema;
    }
}
