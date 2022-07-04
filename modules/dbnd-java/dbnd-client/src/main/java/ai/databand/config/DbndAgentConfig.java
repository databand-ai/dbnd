package ai.databand.config;

import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.DBND__SPARK__IO_TRACKING_ENABLED;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__VERBOSE;

public class DbndAgentConfig {

    private final Map<String, String> properties;

    public DbndAgentConfig(String args) {
        properties = new HashMap<>(1);
        if (args == null) {
            return;
        }

        args = args.trim();
        if (args.isEmpty()) {
            return;
        }

        for (String argPair : args.split(",")) {
            String[] keyValue = argPair.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Arguments for the agent should be like: key1=value1,key2=value2");
            }

            String key = keyValue[0].trim();
            if (key.isEmpty()) {
                throw new IllegalArgumentException("Argument key should not be empty");
            }

            properties.put(key, keyValue[1].trim());
        }
    }

    protected final boolean isTrue(String key) {
        return Boolean.TRUE.toString().equalsIgnoreCase(properties.get(key));
    }

    public boolean isVerbose() {
        return isTrue(DBND__TRACKING__VERBOSE);
    }

    public boolean sparkIoTrackingEnabled() {
        return isTrue(DBND__SPARK__IO_TRACKING_ENABLED);
    }
}
