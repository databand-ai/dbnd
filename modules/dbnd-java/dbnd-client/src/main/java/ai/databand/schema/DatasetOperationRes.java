package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DatasetOperationRes {

    private String latestOperationStatus;
    private String latestOperationError;
    private long records;
    private long operations;
    private String datasetPath;
    private String taskRunUid;
    private String operationType;
    private String taskRunName;

    public String getLatestOperationStatus() {
        return latestOperationStatus;
    }

    public void setLatestOperationStatus(String latestOperationStatus) {
        this.latestOperationStatus = latestOperationStatus;
    }

    public String getLatestOperationError() {
        return latestOperationError;
    }

    public void setLatestOperationError(String latestOperationError) {
        this.latestOperationError = latestOperationError;
    }

    public long getRecords() {
        return records;
    }

    public void setRecords(long records) {
        this.records = records;
    }

    public long getOperations() {
        return operations;
    }

    public void setOperations(long operations) {
        this.operations = operations;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public void setDatasetPath(String datasetPath) {
        this.datasetPath = datasetPath;
    }

    public String getTaskRunUid() {
        return taskRunUid;
    }

    public void setTaskRunUid(String taskRunUid) {
        this.taskRunUid = taskRunUid;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public String getTaskRunName() {
        return taskRunName;
    }

    public void setTaskRunName(String taskRunName) {
        this.taskRunName = taskRunName;
    }
}
