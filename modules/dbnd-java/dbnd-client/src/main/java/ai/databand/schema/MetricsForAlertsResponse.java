package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricsForAlertsResponse {

    private List<MetricForAlerts> data;

    public List<MetricForAlerts> getData() {
        return data;
    }

    public void setData(List<MetricForAlerts> data) {
        this.data = data;
    }
}
