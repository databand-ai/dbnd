/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.examples;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

public class ScalaPipelinesTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }
        Logger.getLogger("ai.databand").setLevel(Level.INFO);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);
    }

    @Test
    public void testScalaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now().minusSeconds(1L);
        String input = getClass().getClassLoader().getResource("p_a_master_data.csv").getFile();
        String output = System.getenv("PROCESS_DATA_OUTPUT") + "/scala_pipeline";
        ScalaSparkPipeline.main(new String[]{input, output});
        new PipelinesVerify().verifyOutputs("scala_spark_pipeline", now, "scala_spark_pipeline");
    }
}
