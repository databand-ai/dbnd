/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

class DbndWrapperTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }
        Logger.getLogger("ai.databand").setLevel(Level.DEBUG);
    }

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

    @Test
    public void runNameIsDifferentAfterCleanup() {
        String runName = DbndWrapper.instance().config().runName();
        DbndWrapper.instance().cleanup();
        String newRunName = DbndWrapper.instance().config().runName();
        MatcherAssert.assertThat(
            "Run names should be different after calling afterPipeline() method",
            runName,
            Matchers.not(Matchers.equalTo(newRunName)));
    }
}
