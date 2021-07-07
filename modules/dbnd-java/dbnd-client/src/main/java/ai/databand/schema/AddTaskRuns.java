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
