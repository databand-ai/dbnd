package ai.databand.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * JAVA_OPTS properties source. Variables are passed in lowercase+dot format:
 * java -jar ... -Ddbnd.tracking.enabled=True
 */
public class JavaOpts implements PropertiesSource {

    private final Map<String, String> props;

    public JavaOpts() {
        this(new SimpleProps());
    }

    public JavaOpts(PropertiesSource parent) {
        this(
            parent,
            System.getProperties()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()))
        );
    }

    public JavaOpts(Map<String, String> systemProps) {
        this(new SimpleProps(), systemProps);
    }

    public JavaOpts(PropertiesSource parent, Map<String, String> systemProps) {
        props = new HashMap<>(parent.values());
        props.putAll(new NormalizedProps(systemProps).values());
    }

    public Map<String, String> values() {
        return Collections.unmodifiableMap(props);
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }


}
