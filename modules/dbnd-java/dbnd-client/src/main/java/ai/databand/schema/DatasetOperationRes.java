/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DatasetOperationRes {

    private String latestOperationStatus;
    private long records;
    private long operations;
    private String datasetPath;
    private String taskRunUid;
    private String operationType;
    private String operationSource;
    private String taskRunName;
    private List<Issue> issues;
    private List<ColumnStats> columnsStats;

    public String getLatestOperationStatus() {
        return latestOperationStatus;
    }

    public void setLatestOperationStatus(String latestOperationStatus) {
        this.latestOperationStatus = latestOperationStatus;
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

    public String getOperationSource() {
        return operationSource;
    }

    public void setOperationSource(String operationSource) {
        this.operationSource = operationSource;
    }

    public String getTaskRunName() {
        return taskRunName;
    }

    public void setTaskRunName(String taskRunName) {
        this.taskRunName = taskRunName;
    }

    public List<Issue> getIssues() {
        return issues;
    }

    public void setIssues(List<Issue> issues) {
        this.issues = issues;
    }

    public List<ColumnStats> getColumnsStats() {
        return columnsStats;
    }

    public void setColumnsStats(List<ColumnStats> columnsStats) {
        this.columnsStats = columnsStats;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Issue {

        private String type;
        private Data data;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Data getData() {
            return data;
        }

        public void setData(Data data) {
            this.data = data;
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Data {
        private String operationError;

        public String getOperationError() {
            return operationError;
        }

        public void setOperationError(String operationError) {
            this.operationError = operationError;
        }


    }
}
