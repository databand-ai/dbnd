package ai.databand.azkaban;

import ai.databand.config.JavaOpts;
import ai.databand.config.PropertiesSource;
import ai.databand.config.SimpleProps;
import azkaban.server.AzkabanServer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AzkabanProps implements PropertiesSource {

    private final Map<String, String> props;

    public AzkabanProps() {
        this(new SimpleProps());
    }

    public AzkabanProps(PropertiesSource parent) {
        props = new HashMap<>(parent.values());
        JavaOpts javaOpts = new JavaOpts(AzkabanServer.getAzkabanProperties().getFlattened());
        props.putAll(javaOpts.values());
    }

    @Override
    public Map<String, String> values() {
        return props;
    }

    @Override
    public Optional<String> getValue(String key) {
        return Optional.ofNullable(props.get(key));
    }

}
