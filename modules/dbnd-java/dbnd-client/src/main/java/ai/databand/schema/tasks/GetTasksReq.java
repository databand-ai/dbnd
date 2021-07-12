package ai.databand.schema.tasks;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GetTasksReq {

    @JsonProperty("uids")
    private List<String> taskUids;

    public GetTasksReq(List<String> taskUids) {
        this.taskUids = taskUids;
    }

    public List<String> getTaskUids() {
        return taskUids;
    }
}
