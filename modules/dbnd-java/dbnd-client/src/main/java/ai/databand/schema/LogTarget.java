/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import java.util.List;

public class LogTarget {

    private final String runUid;
    private final String taskRunUid;
    private final String taskRunName;
    private final String taskRunAttemptUid;
    private final String targetPath;
    private final String paramName;
    private final String taskDefUid;
    private final String operationType;
    private final String operationStatus;
    private final String valuePreview;
    private final List<Long> dataDimensions;
    private final Object dataSchema;
    private final String dataHash;

    public LogTarget(String runUid,
                     String taskRunUid,
                     String taskRunName,
                     String taskRunAttemptUid,
                     String targetPath,
                     String paramName,
                     String taskDefUid,
                     String operationType,
                     String operationStatus,
                     String valuePreview,
                     List<Long> dataDimensions,
                     Object dataSchema,
                     String dataHash) {
        this.runUid = runUid;
        this.taskRunUid = taskRunUid;
        this.taskRunName = taskRunName;
        this.taskRunAttemptUid = taskRunAttemptUid;
        this.targetPath = targetPath;
        this.paramName = paramName;
        this.taskDefUid = taskDefUid;
        this.operationType = operationType;
        this.operationStatus = operationStatus;
        this.valuePreview = valuePreview;
        this.dataDimensions = dataDimensions;
        this.dataSchema = dataSchema;
        this.dataHash = dataHash;
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

    public String getTargetPath() {
        return targetPath;
    }

    public String getParamName() {
        return paramName;
    }

    public String getTaskDefUid() {
        return taskDefUid;
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

    public Object getDataSchema() {
        return dataSchema;
    }

    public String getDataHash() {
        return dataHash;
    }
}
