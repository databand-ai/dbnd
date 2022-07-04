package ai.databand.examples;

import ai.databand.config.DbndConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

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
        pipelinesVerify.verifyOutputs(jobName, now, true, true, "spark_scala_pipeline", true);
    }
}
