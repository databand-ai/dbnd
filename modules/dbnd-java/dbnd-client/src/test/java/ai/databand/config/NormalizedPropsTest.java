package ai.databand.config;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_URL;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__ENABLED;
import static org.hamcrest.MatcherAssert.assertThat;

class NormalizedPropsTest {

    @Test
    public void testNormalization() {
        Map<String, String> props = new HashMap<>(1);
        props.put("DBND__TRACKING__ENABLED", "True");
        props.put("DBND__CORE__DATABAND_URL", "https://tracker.databand.ai/");
        props.put("SOME_VAR", "FALSE");
        NormalizedProps normalized = new NormalizedProps(props);

        assertThat("DBND property should be normalized", normalized.getValue(DBND__TRACKING__ENABLED).isPresent(), Matchers.equalTo(true));
        assertThat("DBND property should be normalized", normalized.getValue(DBND__CORE__DATABAND_URL).isPresent(), Matchers.equalTo(true));
        assertThat("Non-DBND property should not be normalized", normalized.getValue("SOME_VAR").isPresent(), Matchers.equalTo(true));

        assertThat("DBND property should be normalized", normalized.getValue(DBND__TRACKING__ENABLED).get(), Matchers.equalTo("True"));
        assertThat("DBND property should be normalized", normalized.getValue(DBND__CORE__DATABAND_URL).get(), Matchers.equalTo("https://tracker.databand.ai/"));
        assertThat("Non-DBND property should not be normalized", normalized.getValue("SOME_VAR").get(), Matchers.equalTo("FALSE"));
    }
}
