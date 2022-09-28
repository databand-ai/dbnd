/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import ai.databand.config.NormalizedProps;
import ai.databand.config.PropertiesSource;
import azkaban.execapp.FlowRunner;
import azkaban.utils.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Shared properties loader.
 * Azkaban jobs may be configured using "shared properties" files stored in a flow directory.
 * Those properties are not available in a runtime during flow start.
 * This class has to access private methods to load those properties.
 */
public class AzkabanFlowSharedProps implements PropertiesSource {

    private static final Logger LOG = LoggerFactory.getLogger(AzkabanFlowSharedProps.class);
    private final Map<String, String> props;

    public AzkabanFlowSharedProps(FlowRunner flowRunner) {
        Map<String, String> azkabanProps = new HashMap<>();

        try {
            Field sharedPropsField = flowRunner.getClass().getDeclaredField("sharedProps");
            if (!sharedPropsField.isAccessible()) {
                sharedPropsField.setAccessible(true);
            }
            Map<String, Props> sharedProps = (Map) sharedPropsField.get(flowRunner);
            for (Props nextProps : sharedProps.values()) {
                azkabanProps.putAll(nextProps.getFlattened());
            }
        } catch (IllegalAccessException | NoSuchFieldException e) {
            LOG.error("Unable to load shared properties from the Azkaban Flow", e);
        }
        props = new NormalizedProps(azkabanProps).values();
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
