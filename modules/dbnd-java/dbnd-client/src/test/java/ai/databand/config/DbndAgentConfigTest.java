package ai.databand.config;

import ai.databand.DbndPropertyNames;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DbndAgentConfigTest {

    @Test
    public void testNullArgs() {
        DbndAgentConfig config = new DbndAgentConfig(null);
        assertThat("Wrong config property value", config.isVerbose(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkListenerInjectEnabled(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkQueryListenerInjectEnabled(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkIoTrackingEnabled(), Matchers.equalTo(false));
    }

    @Test
    public void testEmptyArgs() {
        DbndAgentConfig config = new DbndAgentConfig("");
        assertThat("Wrong config property value", config.isVerbose(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkListenerInjectEnabled(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkQueryListenerInjectEnabled(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkIoTrackingEnabled(), Matchers.equalTo(false));
    }

    @Test
    public void testWrongArgs() {
        assertThrows(IllegalArgumentException.class, () -> new DbndAgentConfig("key=value=value"));
    }

    @Test
    public void testEmptyValue() {
        assertThrows(IllegalArgumentException.class, () -> new DbndAgentConfig("key=,key2=value2"));
    }

    @Test
    public void testCorrectnValue() {
        DbndAgentConfig config = new DbndAgentConfig(DbndPropertyNames.DBND__TRACKING__VERBOSE + "=true," + DbndPropertyNames.DBND__SPARK__LISTENER_INJECT_ENABLED + "=true");
        assertThat("Wrong config property value", config.isVerbose(), Matchers.equalTo(true));
        assertThat("Wrong config property value", config.sparkListenerInjectEnabled(), Matchers.equalTo(true));
        assertThat("Wrong config property value", config.sparkQueryListenerInjectEnabled(), Matchers.equalTo(false));
        assertThat("Wrong config property value", config.sparkIoTrackingEnabled(), Matchers.equalTo(false));
    }
}
