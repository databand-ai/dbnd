package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OperationsRes {

    private List<DatasetOperationRes> existingOperations;

    public List<DatasetOperationRes> getExistingOperations() {
        return existingOperations;
    }

    public void setExistingOperations(List<DatasetOperationRes> existingOperations) {
        this.existingOperations = existingOperations;
    }
}
