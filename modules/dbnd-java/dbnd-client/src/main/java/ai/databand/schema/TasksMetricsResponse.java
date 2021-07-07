package ai.databand.schema;

import java.util.List;
import java.util.Map;

public class TasksMetricsResponse {

    private Map<String, Map<String, List<List<Object>>>> metrics;

    public Map<String, Map<String, List<List<Object>>>> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Map<String, List<List<Object>>>> metrics) {
        this.metrics = metrics;
    }
}
