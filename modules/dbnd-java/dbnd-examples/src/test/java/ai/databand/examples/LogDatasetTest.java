package ai.databand.examples;

import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.Job;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

public class LogDatasetTest {

    private static PipelinesVerify pipelinesVerify;

    @BeforeAll
    static void beforeAll() throws Exception {
        pipelinesVerify = new PipelinesVerify(new ApiWithTokenBuilder().api());
    }

    /**
     * Scala pipeline test uses auto-inject listener.
     *
     * @throws IOException
     */
    @Test
    public void testScalaPipeline() throws IOException {
        ZonedDateTime now = ZonedDateTime.now().minusSeconds(1L);
        LogDatasetPipeline.main(new String[]{});

        String jobName = "log_dataset_pipeline";
        Job job = pipelinesVerify.verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = pipelinesVerify.verifyTasks(jobName, job);
        TaskFullGraph graph = tasksAndGraph.right();
        Tasks tasks = tasksAndGraph.left();

        TaskRun main = pipelinesVerify.assertTaskExists(jobName + "-parent", tasks, "success");
        Map<String, List<DatasetOperationRes>> datasetOpsByTask = pipelinesVerify.fetchDatasetOperations(job);

        DatasetOperationRes op = pipelinesVerify.assertDatasetOperationExists(
            "log_dataset_pipeline",
            "s3://databand/samples/usa-education-budget.csv",
            DatasetOperationType.READ,
            "SUCCESS",
            41,
            1,
            datasetOpsByTask
        );

        pipelinesVerify.assertColumnStat(
            op.getColumnsStats(),
            "RATIO",
            "double",
            41,
            39,
            0.44597560,
            0.237,
            0.915,
            0.1158023,
            0.383,
            0.421,
            0.496
        );


        pipelinesVerify.assertColumnStat(
            op.getColumnsStats(),
            "BUDGET_ON_EDUCATION",
            "double",
            41,
            41,
            40138.12195121951,
            9314.0,
            131891.0,
            27530.137729582035,
            18952.0,
            31816.0,
            57458.0
        );
    }
}
