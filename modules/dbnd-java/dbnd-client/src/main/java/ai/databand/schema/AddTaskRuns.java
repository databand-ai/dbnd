/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public class AddTaskRuns {

    private final TaskRunsInfo taskRunsInfo;

    public AddTaskRuns(TaskRunsInfo taskRunsInfo) {
        this.taskRunsInfo = taskRunsInfo;
    }

    public TaskRunsInfo getTaskRunsInfo() {
        return taskRunsInfo;
    }
}
