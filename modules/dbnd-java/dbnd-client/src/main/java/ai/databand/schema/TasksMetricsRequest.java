package ai.databand.schema;

import java.util.List;

public class TasksMetricsRequest {

    private final List<String> uids;

    public TasksMetricsRequest(List<String> uids) {
        this.uids = uids;
    }

    public List<String> getUids() {
        return uids;
    }
}
