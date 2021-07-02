package ai.databand.azkaban;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class AzkabanDbndConfigTest {


    @Test
    public void testEmptyProps() {
        AzkabanDbndConfig conf = new AzkabanDbndConfig("", "");

        assertThat("Project should be allowed to track", conf.isTrackingEnabled("project", "flow"), equalTo(true));
        assertThat("Project should be allowed to track", conf.isTrackingEnabled("bad_project", "flow"), equalTo(true));
        assertThat("Project should be allowed to track", conf.isTrackingEnabled("another_project", "flow2"), equalTo(true));
    }

    @Test
    public void testEmptyFlows() {
        AzkabanDbndConfig conf = new AzkabanDbndConfig("project", "");

        assertThat("Project should be allowed to track", conf.isTrackingEnabled("project", "flow"), equalTo(true));
        assertThat("Project should not be allowed to track", conf.isTrackingEnabled("bad_project", "flow"), equalTo(false));
    }

    @Test
    public void testEmptyProjects() {
        AzkabanDbndConfig conf = new AzkabanDbndConfig("", "good,bad");

        assertThat("Flow should be allowed to track", conf.isTrackingEnabled("project", "good"), equalTo(true));
        assertThat("Flow should be allowed to track", conf.isTrackingEnabled("project2", "bad"), equalTo(true));
        assertThat("Flow should not be allowed to track", conf.isTrackingEnabled("project", "ugly"), equalTo(false));
    }

    @Test
    public void testNullProps() {
        AzkabanDbndConfig conf = new AzkabanDbndConfig(null, null);

        assertThat("Project should be allowed to track", conf.isTrackingEnabled("project", "flow"), equalTo(true));
        assertThat("Project should be allowed to track", conf.isTrackingEnabled("bad_project", "flow"), equalTo(true));
        assertThat("Project should be allowed to track", conf.isTrackingEnabled("another_project", "flow2"), equalTo(true));
    }

    @Test
    public void testFilledProps() {
        AzkabanDbndConfig conf = new AzkabanDbndConfig("databand,animals", "run-spark,run-animals");

        assertThat("Project should be allowed to track", conf.isTrackingEnabled("databand", "job"), equalTo(true));
        assertThat("Project should not be allowed to track", conf.isTrackingEnabled("bad_project", "job"), equalTo(false));
        assertThat("Job should not be allowed to track", conf.isTrackingEnabled("bad_project", "run-spark"), equalTo(true));

        assertThat("Job should be allowed to track", conf.isTrackingEnabled("animals", "cheetah"), equalTo(true));
        assertThat("Job should be allowed to track", conf.isTrackingEnabled("zoo", "run-animals"), equalTo(true));
    }

}
