/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import java.util.List;

public class RunAndDefinition {

    private final TaskRun taskRun;
    private final TaskDefinition taskDefinition;
    private final List<LogTarget> targets;

    public RunAndDefinition(TaskRun taskRun, TaskDefinition taskDefinition, List<LogTarget> targets) {
        this.taskRun = taskRun;
        this.taskDefinition = taskDefinition;
        this.targets = targets;
    }

    public TaskRun taskRun() {
        return taskRun;
    }

    public TaskDefinition taskDefinition() {
        return taskDefinition;
    }

    public List<LogTarget> targets() {
        return targets;
    }
}
