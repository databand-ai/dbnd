package ai.databand.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

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
        boolean isNextPropsFilePath = false;
        for (String next : command.split(" ")) {
            if (isNextPropsFilePath) {
                for (Map.Entry<Object, Object> property : readPropertiesFile(next).entrySet()) {
                    String key = (String) property.getKey();
                    if (key.startsWith("spark.env.")) {
                        sparkProps.put(key.replace("spark.env.", ""), (String) property.getValue());
                    }
                }
                isNextPropsFilePath = false;
            }
            if (next.startsWith("--properties-file")) {
                isNextPropsFilePath = true;
            }
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

    private Properties readPropertiesFile(String propertiesPath) {
        Properties properties = new Properties();
        if (propertiesPath != null && Files.exists(Paths.get(propertiesPath))) {
            try (InputStream input = new FileInputStream(propertiesPath)) {
                properties.load(input);
            } catch (IOException e) {
                System.out.println("Unable to read Spark properties from file " + propertiesPath);
                e.printStackTrace();
            }
        }
        return properties;
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }

}
