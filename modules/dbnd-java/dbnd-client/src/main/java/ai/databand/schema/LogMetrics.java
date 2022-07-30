/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import java.util.List;

public class LogMetrics {

    private final List<LogMetric> metricsInfo;

    public LogMetrics(List<LogMetric> metricsInfo) {
        this.metricsInfo = metricsInfo;
    }

    public List<LogMetric> getMetricsInfo() {
        return metricsInfo;
    }
}
