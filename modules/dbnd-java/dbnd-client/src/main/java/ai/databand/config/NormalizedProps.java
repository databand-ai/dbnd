/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Normalizes properties. DBND__TRACKING__ENABLED to dbnd.tracking.enabled.
 */
public class NormalizedProps implements PropertiesSource {

    private final Map<String, String> props;

    public NormalizedProps(Map<String, String> propsToNormalize) {
        props = new HashMap<>();
        for (Map.Entry<String, String> prop : propsToNormalize.entrySet()) {
            String key = prop.getKey();
            String normalizedValue = prop.getValue().trim();
            if (key.toLowerCase().startsWith("dbnd")) {
                String normalizedKey = key.replace("__", ".").toLowerCase();
                props.put(normalizedKey, normalizedValue);
            } else {
                props.put(key, prop.getValue());
            }
        }
    }

    @Override
    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }

}
