package ai.databand.schema;

import java.util.List;

public class UpdateTaskRunAttempts {

    private final List<TaskRunAttemptUpdate> taskRunAttemptUpdates;

    public UpdateTaskRunAttempts(List<TaskRunAttemptUpdate> taskRunAttemptUpdates) {
        this.taskRunAttemptUpdates = taskRunAttemptUpdates;
    }

    public List<TaskRunAttemptUpdate> getTaskRunAttemptUpdates() {
        return taskRunAttemptUpdates;
    }
}
