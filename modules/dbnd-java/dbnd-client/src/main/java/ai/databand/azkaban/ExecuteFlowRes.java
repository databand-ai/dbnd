/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecuteFlowRes {

    private String message;

    private String project;

    private String flow;

    @JsonProperty("execid")
    private int execId;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public int getExecId() {
        return execId;
    }

    public void setExecId(int execId) {
        this.execId = execId;
    }

    public boolean isSuccess() {
        return message != null && message.contains("successfully");
    }
}
