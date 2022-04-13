package ai.databand.examples;

import ai.databand.azkaban.AzkabanClient;
import ai.databand.azkaban.FetchFlowExecutionRes;
import ai.databand.config.DbndConfig;
import ai.databand.schema.Job;
import ai.databand.schema.Pair;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.Tasks;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.fail;

public class AzkabanPipelinesTest {

    private static PipelinesVerify pipelinesVerify;

    @BeforeAll
    static void beforeAll() throws Exception {
        pipelinesVerify = new PipelinesVerify(new ApiWithTokenBuilder().api());
    }

    @Test
    public void testAzkabanAnimalFlow() throws IOException, URISyntaxException {
        Optional<AzkabanClient> clientOpt = getClient();
        if (!clientOpt.isPresent()) {
            System.out.println("Skipping azkaban tests execution because azkaban.url property wasn't set");
            return;
        }
        AzkabanClient client = clientOpt.get();

        String sessionId = login(client);

        String projectName = "animal-flow";
        String databandPipelineName = "animal-flow__animals";
        String azkabanFlowName = "animals";

        List<String> jobs = createProject(projectName, client, sessionId);

        Optional<Integer> execId = client.executeFlow(sessionId, projectName, azkabanFlowName);
        assertThat("Flow should be executed", execId.isPresent(), Matchers.equalTo(true));

        waitForFlowCompletion(client, sessionId, execId.get());

        verifyJob(databandPipelineName, jobs, azkabanFlowName, execId.get(), projectName);
    }

    @Test
    public void testSparkFlow() throws IOException, URISyntaxException {
        ZonedDateTime now = ZonedDateTime.now().minusSeconds(1L);
        String sparkJobJarPath = System.getenv("EXAMPLES_JAR");
        if (sparkJobJarPath == null || sparkJobJarPath.isEmpty()) {
            System.out.println("Skipping azkaban+spark tests execution because EXAMPLES_JAR property wasn't set");
            return;
        }
        Optional<AzkabanClient> clientOpt = getClient();
        if (!clientOpt.isPresent()) {
            System.out.println("Skipping azkaban tests execution because azkaban.url property wasn't set");
            return;
        }
        AzkabanClient client = clientOpt.get();

        String sessionId = login(client);

        String projectName = "spark-flow";
        String databandPipelineName = "spark-flow__generate-report";
        String azkabanFlowName = "generate-report";

        Path examplesJar = Paths.get(System.getenv("EXAMPLES_JAR"));
        Path sampleJson = Paths.get(getClass().getClassLoader().getResource("sample.json").toURI());

        List<String> jobs = createProject(projectName, client, sessionId, Arrays.asList(examplesJar, sampleJson));

        Optional<Integer> execId = client.executeFlow(sessionId, projectName, azkabanFlowName);
        assertThat("Flow should be executed", execId.isPresent(), Matchers.equalTo(true));

        waitForFlowCompletion(client, sessionId, execId.get());

        verifyJob(databandPipelineName, jobs, azkabanFlowName, execId.get(), projectName);

        pipelinesVerify.verifyOutputs("spark-flow__generate-report", now, true, false, "spark_scala_pipeline", false);
    }

    public Optional<AzkabanClient> getClient() {
        DbndConfig config = new DbndConfig();
        Optional<String> azkabanUrl = config.getValue("AZKABAN_URL");
        return azkabanUrl.map(AzkabanClient::new);
    }

    public String login(AzkabanClient client) {
        Optional<String> login = client.login("azkaban", "azkaban");
        assertThat("Login should be successful", login.isPresent(), Matchers.equalTo(true));
        return login.get();
    }

    public List<String> createProject(String projectName,
                                      AzkabanClient client,
                                      String sessionId) throws IOException, URISyntaxException {
        return createProject(projectName, client, sessionId, Collections.emptyList());
    }

    public List<String> createProject(String projectName,
                                      AzkabanClient client,
                                      String sessionId,
                                      List<Path> filesToAdd) throws IOException, URISyntaxException {
        Optional<String> project = client.createProject(sessionId, projectName, "databand description");

        assertThat("Project should be created", project.isPresent(), Matchers.equalTo(true));

        URI flowDirectory = getClass().getClassLoader().getResource(String.format("azkaban/%s", projectName)).toURI();

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        List<String> jobs = new ArrayList<>(1);

        Path source = Paths.get(flowDirectory);
        try (ZipOutputStream zs = new ZipOutputStream(out);
             Stream<Path> paths = Files.walk(source)) {
            paths.filter(path -> !Files.isDirectory(path))
                .forEach(path -> {
                    jobs.add(path.getFileName().toString().replace(".job", ""));
                    ZipEntry zipEntry = new ZipEntry(source.relativize(path).toString());
                    try {
                        zs.putNextEntry(zipEntry);
                        Files.copy(path, zs);
                        zs.closeEntry();
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to create a zip file with flow");
                    }
                });
            for (Path fileToAdd : filesToAdd) {
                ZipEntry additionalFile = new ZipEntry(fileToAdd.getFileName().toString());
                zs.putNextEntry(additionalFile);
                Files.copy(fileToAdd, zs);
                zs.closeEntry();
            }
        }

        byte[] zippedFile = out.toByteArray();

        boolean uploadResult = client.uploadProject(sessionId, projectName, zippedFile);
        assertThat("Flow file should be uploaded", uploadResult, Matchers.equalTo(true));
        return jobs;
    }

    public void waitForFlowCompletion(AzkabanClient client, String sessionId, Integer execId) {
        // wait for 20 seconds until flow finishes
        try {
            int tryNumber = 0;
            while (tryNumber < 60) {
                Optional<FetchFlowExecutionRes> flow = client.fetchFlowExecution(sessionId, execId);

                if (flow.isPresent() && flow.get().isFailed()) {
                    fail("Flow is failed. Check logs for details");
                    break;
                }

                if (!flow.isPresent() || !flow.get().isCompleted()) {
                    tryNumber++;
                    System.out.printf("Waiting until flow will be completed, try #%s%n", tryNumber);
                    Thread.sleep(1_000);
                } else {
                    break;
                }
            }
        } catch (InterruptedException e) {
            fail("Flow completing waiting was interrupted");
        }
    }

    public void verifyJob(String databandPipelineName,
                          List<String> jobs,
                          String azkabanFlowName,
                          Integer execId,
                          String projectName) throws IOException {
        Job job = pipelinesVerify.verifyJob(databandPipelineName);

        Pair<Tasks, TaskFullGraph> pair = pipelinesVerify.verifyTasks(databandPipelineName, job);

        List<String> tasksNames = fetchTasksByState(pair.left(), "success");
        List<String> failedTasks = fetchTasksByState(pair.left(), "failed");

        assertThat("Some tasks weren't completed", tasksNames.size() + failedTasks.size(), Matchers.equalTo(pair.left().getTaskInstances().size()));

        // because root task occurs twice in result
        jobs.add(azkabanFlowName);

        for (String nextJob : jobs) {
            assertThat("All tasks should succeed.", tasksNames, hasItem(nextJob));
        }

        String rootTaskRunUid = pair.right().getRootTaskRunUid();
        TaskRun rootTaskRun = pair.left().getTaskInstances().get(rootTaskRunUid);

        pipelinesVerify.assertParamInTask(rootTaskRun, "azkaban.flow.flowid", azkabanFlowName);
        pipelinesVerify.assertParamInTask(rootTaskRun, "azkaban.flow.execid", String.valueOf(execId));
        pipelinesVerify.assertParamInTask(rootTaskRun, "azkaban.flow.projectname", projectName);
        pipelinesVerify.assertParamInTask(rootTaskRun, "azkaban.flow.submituser", "azkaban");

        pipelinesVerify.assertLogsInTask(rootTaskRun.getLatestTaskRunAttemptId(), String.format("Running flow '%s'", azkabanFlowName));
        pipelinesVerify.assertProjectName(rootTaskRun.getRunUid(), projectName);
    }

    private List<String> fetchTasksByState(Tasks tasks, String state) {
        return tasks
            .getTaskInstances()
            .values()
            .stream()
            .filter(taskRun -> state.equalsIgnoreCase(taskRun.getState()))
            .map(TaskRun::getName)
            .collect(Collectors.toList());
    }

}
