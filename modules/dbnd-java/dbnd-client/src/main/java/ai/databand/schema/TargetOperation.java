/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetOperation {

    private String path;
    private String targetUid;
    private String dataHash;
    private String taskRunParamUid;
    private String paramName;
    private String taskRunUid;
    private String runUid;
    private String taskRunName;
    private String valuePreview;
    private String operationType;
    private String uid;
    private String dataSchema;
    private List<Long> dataDimensions;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getTargetUid() {
        return targetUid;
    }

    public void setTargetUid(String targetUid) {
        this.targetUid = targetUid;
    }

    public String getDataHash() {
        return dataHash;
    }

    public void setDataHash(String dataHash) {
        this.dataHash = dataHash;
    }

    public String getTaskRunParamUid() {
        return taskRunParamUid;
    }

    public void setTaskRunParamUid(String taskRunParamUid) {
        this.taskRunParamUid = taskRunParamUid;
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getTaskRunUid() {
        return taskRunUid;
    }

    public void setTaskRunUid(String taskRunUid) {
        this.taskRunUid = taskRunUid;
    }

    public String getRunUid() {
        return runUid;
    }

    public void setRunUid(String runUid) {
        this.runUid = runUid;
    }

    public String getTaskRunName() {
        return taskRunName;
    }

    public void setTaskRunName(String taskRunName) {
        this.taskRunName = taskRunName;
    }

    public String getValuePreview() {
        return valuePreview;
    }

    public void setValuePreview(String valuePreview) {
        this.valuePreview = valuePreview;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getDataSchema() {
        return dataSchema;
    }

    public void setDataSchema(String dataSchema) {
        this.dataSchema = dataSchema;
    }

    public List<Long> getDataDimensions() {
        return dataDimensions;
    }

    public void setDataDimensions(List<Long> dataDimensions) {
        this.dataDimensions = dataDimensions;
    }
}
