/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.mlflow;

import ai.databand.ApiWithTokenBuilder;
import ai.databand.DbndApi;
import ai.databand.config.DbndConfig;
import ai.databand.schema.NodeInfo;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TasksMetricsRequest;
import ai.databand.schema.TasksMetricsResponse;
import org.junit.jupiter.api.Test;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.MlflowClient;
import retrofit2.Response;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbndMlflowClientTest {

    @Test
    public void testUuid5() {
        DbndConfig config = new DbndConfig();
        String trackerUrl = config.databandUrl();
        if (trackerUrl == null) {
            trackerUrl = "http://localhost:8080";
        }

        MlflowClient stubMlflowClient = new StubMlFlowClient();
        DbndMlflowClient dbnd = new DbndMlflowClient(stubMlflowClient, trackerUrl);

        String s = dbnd.uuid5("namespace", "name");

        assertNotNull(s, "Shouldn't be null");

        String s2 = dbnd.uuid5("namespace", "name");

        assertEquals(s, s2);
    }

    @Test
    public void testSendMetric() throws IOException {
        DbndConfig config = new DbndConfig();
        String trackerUrl = config.databandUrl();
        if (trackerUrl == null) {
            trackerUrl = "http://localhost:8080";
        }

        MlflowClient stubMlflowClient = new StubMlFlowClient();
        DbndMlflowClient dbnd = new DbndMlflowClient(stubMlflowClient, trackerUrl);

        DbndApi dbndApi = new ApiWithTokenBuilder().api();

        SecureRandom random = new SecureRandom();

        String experimentId = UUID.randomUUID().toString();

        String runUid = dbnd.uuid5("RUN_UID", experimentId);

        String taskRunUid = dbnd.uuid5("DRIVER_TASK", experimentId);

        Service.RunInfo runInfo = dbnd.createRun(experimentId);

        Response<TaskFullGraph> fullGraphRes = dbndApi.taskFullGraph("embed_mlflow", runUid).execute();

        assertTrue(fullGraphRes.isSuccessful());

        TaskFullGraph fullGraph = fullGraphRes.body();

        assertNotNull(fullGraph);
        assertEquals(taskRunUid, fullGraph.getRootTaskRunUid());

        String firstMetricName = randomString();
        String secondMetricName = randomString();

        double firstMetricValue = random.nextDouble() * 100;
        double secondMetricValue = random.nextDouble() * 100;

        dbnd.logMetric(runInfo.getExperimentId(), firstMetricName, firstMetricValue, Instant.now().toEpochMilli(), 0);
        dbnd.logMetric(runInfo.getExperimentId(), secondMetricName, secondMetricValue, Instant.now().toEpochMilli(), 0);


        Response<TasksMetricsResponse> metricsRes = dbndApi.tasksMetrics(new TasksMetricsRequest(Collections.singletonList(taskRunUid))).execute();

        assertTrue(metricsRes.isSuccessful());

        TasksMetricsResponse taskMetrics = metricsRes.body();

        assertNotNull(taskMetrics);

        Map<String, Map<String, List<List<Object>>>> metricsMap = taskMetrics.getMetrics();
        assertNotNull(metricsMap);

        String taskAfId = "mlflow_driver__" + dbnd.sha1short("TASK_AF", experimentId);

        assertTrue(metricsMap.containsKey(taskAfId));

        Map<String, List<List<Object>>> metricValues = metricsMap.get(taskAfId);

        assertTrue(metricValues.containsKey(firstMetricName));
        assertTrue(metricValues.containsKey(secondMetricName));

        assertFalse(metricValues.get(firstMetricName).isEmpty());
        List<Object> firstValue = metricValues.get(firstMetricName).get(0);
        assertEquals(3, firstValue.size());
        assertEquals(String.valueOf(firstMetricValue).substring(0, 5), firstValue.get(1).toString().substring(0, 5));

        assertFalse(metricValues.get(secondMetricName).isEmpty());
        List<Object> secondValue = metricValues.get(secondMetricName).get(0);
        assertEquals(3, secondValue.size());

        assertEquals(String.valueOf(secondMetricValue).substring(0, 5), secondValue.get(1).toString().substring(0, 5));

        dbnd.setTerminated(runInfo.getExperimentId());

        fullGraphRes = dbndApi.taskFullGraph("embed_mlflow", runUid).execute();

        assertTrue(fullGraphRes.isSuccessful());

        fullGraph = fullGraphRes.body();

        assertNotNull(fullGraph);
        assertEquals(taskRunUid, fullGraph.getRootTaskRunUid());
        assertNotNull(fullGraph.getNodesInfo());
        assertTrue(fullGraph.getNodesInfo().containsKey(String.valueOf(fullGraph.getRoot())));

        NodeInfo root = fullGraph.getNodesInfo().get(String.valueOf(fullGraph.getRoot()));

        assertEquals("success", root.getState());
    }

    private String randomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        SecureRandom random = new SecureRandom();

        return random.ints(leftLimit, rightLimit + 1)
            .limit(targetStringLength)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    private static class StubMlFlowClient extends MlflowClient {

        public StubMlFlowClient() {
            super("http://127.0.0.1");
        }

        @Override
        public void logMetric(String runId, String key, double value, long timestamp, long step) {
            // do nothing
        }

        @Override
        public void setTerminated(String runId) {
            // do nothing
        }

        @Override
        public Service.RunInfo createRun(Service.CreateRun request) {
            Service.RunInfo.Builder builder = Service.RunInfo.newBuilder();
            builder.setExperimentId(request.getExperimentId());
            builder.setStartTime(request.getStartTime());
            builder.setUserId(request.getUserId());
            return builder.build();
        }
    }
}
