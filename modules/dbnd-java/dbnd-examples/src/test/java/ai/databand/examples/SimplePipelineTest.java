package ai.databand.examples;

import ai.databand.DbndApi;
import ai.databand.schema.Job;
import ai.databand.schema.NodeInfo;
import ai.databand.schema.PaginatedData;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.TaskRunParam;
import ai.databand.schema.Tasks;
import ai.databand.schema.tasks.GetTasksReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import retrofit2.Response;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class SimplePipelineTest {

    @Test
    public void testPipeline() throws IOException {
        SecureRandom random = new SecureRandom();
        int firstValue = random.nextInt(100000);
        String secondValue = String.valueOf(random.nextInt(100000));

        SimplePipeline pipeline = new SimplePipeline(firstValue, secondValue);
        pipeline.doStuff();

        DbndApi api = new ApiWithTokenBuilder().api();

        Response<PaginatedData<Job>> jobsRes = api.jobs().execute();

        List<Job> jobs = jobsRes.body().getData();

        String jobName = "jvm_pipeline";
        Optional<Job> jvmJobOpt = jobs.stream()
            .filter(j -> jobName.equals(j.getName()))
            .findFirst();

        Assertions.assertTrue(jvmJobOpt.isPresent(), "Pipeline " + jobName + " does not exists. There may be API compatibility issue");

        Job job = jvmJobOpt.get();

        Response<TaskFullGraph> graphRes = api.taskFullGraph(jobName, job.getLatestRunUid()).execute();

        TaskFullGraph graph = graphRes.body();

        List<String> uids = graph.getNodesInfo().values().stream()
            .map(NodeInfo::getUid)
            .collect(Collectors.toList());

        Response<Tasks> tasksRes = api.tasks(new GetTasksReq(uids)).execute();

        Tasks tasks = tasksRes.body();

        Assertions.assertNotNull(tasks, "Tasks response from dbnd should not be empty.");

        TaskRun firstTask = assertTaskExists("stepOne", tasks);

        assertParamExistsInTask(String.valueOf(firstValue), firstTask);
        assertParamExistsInTask(secondValue, firstTask);

        assertTaskExists("stepTwo", tasks);
    }

    protected TaskRun assertTaskExists(String taskName, Tasks tasks) {
        TaskRun task = tasks.getTaskInstances()
            .values()
            .stream()
            .collect(Collectors.toMap(TaskRun::getTaskId, Function.identity()))
            .get(taskName);
        Assertions.assertNotNull(
            task,
            String.format(
                "Task '%s' should be created in dbnd but wasn't! There may be API compatibility issue. Existing tasks: %s",
                taskName,
                tasks.getTaskInstances().keySet().toString()
            )
        );
        return task;
    }

    protected void assertParamExistsInTask(String paramValue, TaskRun task) {
        Set<String> taskParamValues = task.getTaskRunParams()
            .stream()
            .map(TaskRunParam::getValue)
            .collect(Collectors.toSet());

        Assertions.assertTrue(
            taskParamValues.contains(paramValue),
            String.format("Param value %s does not exists in task run. Existing params: %s", paramValue, taskParamValues)
        );
    }

}
