/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

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
        assertThat("Wrong config property value", config.sparkIoTrackingEnabled(), Matchers.equalTo(false));
    }

    @Test
    public void testEmptyArgs() {
        DbndAgentConfig config = new DbndAgentConfig("");
        assertThat("Wrong config property value", config.isVerbose(), Matchers.equalTo(false));
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
        DbndAgentConfig config = new DbndAgentConfig(DbndPropertyNames.DBND__TRACKING__VERBOSE + "=true");
        assertThat("Wrong config property value", config.isVerbose(), Matchers.equalTo(true));
        assertThat("Wrong config property value", config.sparkIoTrackingEnabled(), Matchers.equalTo(false));
    }
}
