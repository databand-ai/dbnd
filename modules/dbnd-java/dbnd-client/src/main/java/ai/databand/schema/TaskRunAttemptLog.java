/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRunAttemptLog {

    private String taskRunAttemptUid;
    private String taskId;
    private String taskUid;
    private String logBody;

    public String getTaskRunAttemptUid() {
        return taskRunAttemptUid;
    }

    public void setTaskRunAttemptUid(String taskRunAttemptUid) {
        this.taskRunAttemptUid = taskRunAttemptUid;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskUid() {
        return taskUid;
    }

    public void setTaskUid(String taskUid) {
        this.taskUid = taskUid;
    }

    public String getLogBody() {
        return logBody;
    }

    public void setLogBody(String logBody) {
        this.logBody = logBody;
    }
}
