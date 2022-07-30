/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Environment variables properties source. Vars are passed using uppercase+underscore format:
 * DBND__TRACKING__ENABLED=True
 */
public class Env implements PropertiesSource {

    private final Map<String, String> props;

    public Env(PropertiesSource parent) {
        Map<String, String> parentProps = parent.values();
        props = new HashMap<>(parentProps);
        props.putAll(new NormalizedProps(System.getenv()).values());
    }

    public Env() {
        this(new SimpleProps());
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }
}
