package ai.databand;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

class DbndWrapperTest {

    @Test
    public void testMethodWithArgumentsTruncate() {
        String method = "ai.databand.examples.JavaSparkPipeline.execute(java.lang.String)";
        String truncated = DbndWrapper.instance().removeArgsFromMethodName(method);
        assertThat("Wrong truncation result", truncated, Matchers.equalTo("ai.databand.examples.JavaSparkPipeline.execute("));
    }

    @Test
    public void testMethodWithoutArgumentsTruncate() {
        String method = "ai.databand.examples.JavaSparkPipeline.execute";
        String truncated = DbndWrapper.instance().removeArgsFromMethodName(method);
        assertThat("Wrong truncation result", truncated, Matchers.equalTo("ai.databand.examples.JavaSparkPipeline.execute"));
    }

    @Test
    public void testCreateAgentlessRunDbndRunIsNotNull() {
        // setting null context classloader to throw class not found exception
        Thread.currentThread().setContextClassLoader(null);
        DbndWrapper.instance().logMetric("some_metric", "some_value");
        DbndRun run = DbndWrapper.instance().currentRun();
        assertThat("dbnd run is null", run, Matchers.notNullValue());
    }

}
