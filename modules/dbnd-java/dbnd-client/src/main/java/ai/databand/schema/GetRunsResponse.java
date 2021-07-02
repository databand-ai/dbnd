package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetRunsResponse {

    private List<Run> data;

    public List<Run> getData() {
        return data;
    }

    public void setData(List<Run> data) {
        this.data = data;
    }
}
