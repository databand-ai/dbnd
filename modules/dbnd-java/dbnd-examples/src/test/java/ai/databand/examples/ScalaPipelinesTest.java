package ai.databand.examples;

import ai.databand.config.DbndConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

import static ai.databand.DbndPropertyNames.DBND__SPARK__LISTENER_INJECT_ENABLED;
import static ai.databand.DbndPropertyNames.DBND__SPARK__QUERY_LISTENER_INJECT_ENABLED;

public class ScalaPipelinesTest {

    private static PipelinesVerify pipelinesVerify;

    @BeforeAll
    static void beforeAll() throws IOException {
        pipelinesVerify = new PipelinesVerify();
    }

    /**
     * Scala pipeline test uses auto-inject listener.
     *
     * @throws IOException
     */
    @Test
    public void testScalaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now().minusSeconds(1L);
        ScalaSparkPipeline.main(new String[]{});
        DbndConfig config = new DbndConfig();
        String jobName = config.jobName().orElse("spark_scala_pipeline");
        final boolean verifySpark = Boolean.TRUE.toString().equalsIgnoreCase(System.getenv(DBND__SPARK__LISTENER_INJECT_ENABLED));
        final boolean verifyDatasetOps = Boolean.TRUE.toString().equalsIgnoreCase(System.getenv(DBND__SPARK__QUERY_LISTENER_INJECT_ENABLED));
        pipelinesVerify.verifyOutputs(jobName, now, verifySpark, verifyDatasetOps, "spark_scala_pipeline", true);
    }
}
