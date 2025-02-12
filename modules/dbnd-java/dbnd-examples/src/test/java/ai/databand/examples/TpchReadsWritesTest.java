/*
 * © Copyright Databand.ai, an IBM Company 2024
 */

 package ai.databand.examples;

 import ai.databand.schema.DatasetOperationRes;
 import ai.databand.schema.DatasetOperationType;
 import ai.databand.schema.Job;
 import ai.databand.schema.LogDataset;
 import ai.databand.schema.Pair;
 import ai.databand.schema.TaskFullGraph;
 import ai.databand.schema.Tasks;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

 import java.io.IOException;
 import java.util.List;
 import java.util.Map;

 /**
  * This test checks log_dataset_op from listener when performed spark operation submits read operations
  * if there is some reads inside the write plan.
  */
 public class TpchReadsWritesTest {

    @BeforeAll
    static void setup() {
        if(!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
        }
        Logger.getLogger("ai.databand").setLevel(Level.DEBUG);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);
    }

     @Test
     public void testTpcReadsWrites() throws IOException {
         PipelinesVerify pipelinesVerify = new PipelinesVerify();
         TpchExample.main(new String[]{});

         String jobName = "tpch_example";
         Job job = pipelinesVerify.verifyJob(jobName);
         Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
         Tasks tasks = tasksAndGraph.left();

         pipelinesVerify.assertTaskExists(jobName, tasks, "success");
         Map<String, List<DatasetOperationRes>> datasetOpsByTask = pipelinesVerify.fetchDatasetOperations(job);

         pipelinesVerify.assertDatasetOperationExists(
             "tpch_example",
             "build/resources/main/lineitem.csv",
             DatasetOperationType.READ,
             "SUCCESS",
             50,
             1,
             datasetOpsByTask,
             null,
             LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
         );

         pipelinesVerify.assertDatasetOperationExists(
             "tpch_example",
             "build/resources/main/part.csv",
             DatasetOperationType.READ,
             "SUCCESS",
             51,
             1,
             datasetOpsByTask,
             null,
             LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
         );

         pipelinesVerify.assertDatasetOperationExists(
             "tpch_example",
             "build/output/tpch_example",
             DatasetOperationType.WRITE,
             "SUCCESS",
             1,
             1,
             datasetOpsByTask,
             null,
             LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
         );
     }
 }
