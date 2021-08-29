package ai.databand.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Spark config properties source. Values are passed in uppercase+underscore format.
 * TODO: extract spark configuration in runtime.
 */
public class SparkConf implements PropertiesSource {

    private final Map<String, String> props;

    public SparkConf(String command) {
        this(new SimpleProps(), command);
    }

    public SparkConf(PropertiesSource parent) {
        this(parent, System.getProperties().getProperty("sun.java.command"));
    }

    public SparkConf(PropertiesSource parent, String command) {
        props = new HashMap<>(parent.values());
        Map<String, String> sparkProps = new HashMap<>();
        for (String next : command.split(" ")) {
            if (next.startsWith("spark.env.") && next.contains("=")) {
                String[] keyValue = next.split("=");
                if (keyValue.length != 2) {
                    // omit value-less keys
                    continue;
                }
                sparkProps.put(keyValue[0].replace("spark.env.", ""), keyValue[1]);
            }
        }
        props.putAll(new NormalizedProps(sparkProps).values());
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }

}
