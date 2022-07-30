/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tasks {

    private Map<String, TaskRun> taskInstances;
    private List<TargetOperation> targetsOperations;
    private Map<String, List<TaskRunAttempt>> attempts;

    public Map<String, TaskRun> getTaskInstances() {
        return taskInstances;
    }

    public void setTaskInstances(Map<String, TaskRun> taskInstances) {
        this.taskInstances = taskInstances;
    }

    public List<TargetOperation> getTargetsOperations() {
        return targetsOperations;
    }

    public void setTargetsOperations(List<TargetOperation> targetsOperations) {
        this.targetsOperations = targetsOperations;
    }

    public Map<String, List<TaskRunAttempt>> getAttempts() {
        return attempts;
    }

    public void setAttempts(Map<String, List<TaskRunAttempt>> attempts) {
        this.attempts = attempts;
    }
}
