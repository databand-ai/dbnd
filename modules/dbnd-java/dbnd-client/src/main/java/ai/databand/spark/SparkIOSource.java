package ai.databand.spark;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SparkIOSource {

    private static final Logger LOG = LoggerFactory.getLogger(SparkIOSource.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String path;

    private final Map<String, Object> properties;

    private final String metricKey;

    private final String trackingSource;

    public SparkIOSource(String path, String trackingSource, Map<String, Object> properties) {
        this.path = path;
        this.trackingSource = trackingSource;
        this.properties = properties;
        this.metricKey = "spark-io-" + UUID.randomUUID();
    }

    public SparkIOSource(String path, String trackingSource) {
        this(path, trackingSource, Collections.emptyMap());
    }

    public String getPath() {
        return path;
    }

    public String getTrackingSource() {
        return trackingSource;
    }

    @JsonIgnore
    public String metricKey() {
        return metricKey;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String toString() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOG.error("Unable to serialize Spark IO source", e);
            return "{}";
        }
    }
}
