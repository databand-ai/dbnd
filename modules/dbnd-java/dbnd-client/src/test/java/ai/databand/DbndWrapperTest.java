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

}
