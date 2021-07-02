package ai.databand.parameters;

import org.apache.spark.sql.Dataset;

import java.util.HashMap;
import java.util.Map;

public class ParametersPreview {

    private final Map<String, TaskParameterPreview<?>> parameters;
    private final ObjectPreview objectPreview;

    public ParametersPreview(boolean previewEnabled) {
        parameters = new HashMap<>(1);
        if (previewEnabled) {
            parameters.put(String.class.getCanonicalName(), new StringPreview());
            parameters.put(Dataset.class.getCanonicalName(), new DatasetPreview());
            parameters.put(String[].class.getCanonicalName(), new StringArrayPreview());
        } else {
            parameters.put(String.class.getCanonicalName(), new StringPreview());
            parameters.put(String[].class.getCanonicalName(), new StringArrayPreview());
        }
        objectPreview = new ObjectPreview();
    }

    public TaskParameterPreview get(Class<?> clazz) {
        return parameters.getOrDefault(clazz.getCanonicalName(), objectPreview);
    }
}
