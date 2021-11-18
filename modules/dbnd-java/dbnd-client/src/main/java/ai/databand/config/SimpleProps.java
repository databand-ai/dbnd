package ai.databand.config;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class SimpleProps implements PropertiesSource {

    private final Map<String, String> props;

    public SimpleProps(Map<String, String> props) {
        this.props = props;
    }

    public SimpleProps() {
        this.props = Collections.emptyMap();
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
