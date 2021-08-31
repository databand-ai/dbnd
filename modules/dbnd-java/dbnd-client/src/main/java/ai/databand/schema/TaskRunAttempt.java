package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRunAttempt {

    private String id;
    private ErrorInfo error;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public ErrorInfo getError() {
        return error;
    }

    public void setError(ErrorInfo error) {
        this.error = error;
    }
}
