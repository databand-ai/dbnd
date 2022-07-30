/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.examples;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

public class ScalaPipelinesTest {

    @Test
    public void testScalaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now().minusSeconds(1L);
        String input = getClass().getClassLoader().getResource("p_a_master_data.csv").getFile();
        String output = System.getenv("PROCESS_DATA_OUTPUT");
        ScalaSparkPipeline.main(new String[]{input, output});
        new PipelinesVerify().verifyOutputs("scala_spark_pipeline", now, "scala_spark_pipeline");
    }
}
