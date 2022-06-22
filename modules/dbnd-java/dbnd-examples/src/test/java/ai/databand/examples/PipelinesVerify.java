package ai.databand.examples;

import ai.databand.DbndApi;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.ErrorInfo;
import ai.databand.schema.GetRunsResponse;
import ai.databand.schema.Job;
import ai.databand.schema.LogDataset;
import ai.databand.schema.MetricForAlerts;
import ai.databand.schema.MetricsForAlertsResponse;
import ai.databand.schema.NodeInfo;
import ai.databand.schema.NodeRelationInfo;
import ai.databand.schema.PaginatedData;
import ai.databand.schema.Pair;
import ai.databand.schema.Run;
import ai.databand.schema.TargetOperation;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRun;
import ai.databand.schema.TaskRunAttempt;
import ai.databand.schema.TaskRunAttemptLog;
import ai.databand.schema.TaskRunParam;
import ai.databand.schema.Tasks;
import ai.databand.schema.TasksMetricsRequest;
import ai.databand.schema.TasksMetricsResponse;
import ai.databand.schema.histograms.ColumnSummary;
import ai.databand.schema.histograms.NumericSummary;
import ai.databand.schema.histograms.Summary;
import ai.databand.schema.tasks.GetTasksReq;
import org.hamcrest.Matchers;
import retrofit2.Response;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

public class PipelinesVerify {

    private final DbndApi api;

    public PipelinesVerify(DbndApi api) {
        this.api = api;
    }

    protected Job verifyJob(String jobName) throws IOException {
        Response<PaginatedData<Job>> jobsRes = api.jobs().execute();

        PaginatedData<Job> body = jobsRes.body();

        assertThat("Jobs retrieved from databand should not be empty", body, notNullValue());

        List<Job> jobs = body.getData();

        Optional<Job> jvmJobOpt = jobs.stream()
            .filter(j -> jobName.equals(j.getName()))
            .findFirst();

        List<String> jobNames = jobs.stream()
            .map(Job::getName)
            .collect(Collectors.toList());

        assertThat(
            String.format("Pipeline [%s] does not exists. Existing jobs: %s", jobName, jobNames),
            jvmJobOpt.isPresent(),
            equalTo(true)
        );

        return jvmJobOpt.get();
    }

    protected Pair<Tasks, TaskFullGraph> verifyTasks(String jobName, Job job) throws IOException {
        Response<TaskFullGraph> graphRes = api.taskFullGraph(jobName, job.getLatestRunUid()).execute();

        TaskFullGraph graph = graphRes.body();

        List<String> uids = graph.getNodesInfo().values().stream()
            .map(NodeInfo::getUid)
            .collect(Collectors.toList());

        Response<Tasks> tasksRes = api.tasks(new GetTasksReq(uids)).execute();

        Tasks tasks = tasksRes.body();

        assertThat("Tasks response from dbnd should not be empty.", tasks, Matchers.notNullValue());
        return new Pair<>(tasks, graph);
    }

    protected void verifyOutputs(String jobName, ZonedDateTime now, String pipelineName) throws IOException {
        verifyOutputs(jobName, now, true, false, pipelineName, true);
    }

    protected void verifyOutputs(String jobName, ZonedDateTime now, boolean verifySpark, boolean verifyDatasetOps, String pipelineName, boolean verifyAlerts) throws IOException {
        Job job = verifyJob(jobName);
        Pair<Tasks, TaskFullGraph> tasksAndGraph = verifyTasks(jobName, job);
        TaskFullGraph graph = tasksAndGraph.right();
        Tasks tasks = tasksAndGraph.left();

        Map<String, Integer> taskIds = graph.getNodesInfo().values()
            .stream()
            .collect(Collectors.toMap(NodeInfo::getUid, NodeInfo::getId));

        Map<String, Integer> tasksAttemptsIds = tasks.getTaskInstances().values()
            .stream()
            .collect(Collectors.toMap(TaskRun::getUid, TaskRun::getLatestTaskRunAttemptId));

        TaskRun driverTask = assertTaskExists(String.format("%s-parent", pipelineName), tasks, "success");

        TaskRun loadTracks = assertTaskExists("loadTracks", tasks, "success");
        assertThat(
            String.format(
                "Last run was created before the test run, thus, it's not the run we're looking for. Now: %s, run start date: %s",
                now,
                loadTracks.getStartDate()
            ),
            loadTracks.getStartDate().isAfter(now),
            Matchers.is(true)
        );
        TaskRun countTracksByTrackName = assertTaskExists("countTracksByTrackName", tasks, "success");
        TaskRun countTracksByArtist = assertTaskExists("countTracksByArtist", tasks, "success");

        assertTargetOperationExistsInTask(
            pipelineName.contains("scala") ? "arg0" : "path",
            "read",
            tasks.getTargetsOperations(),
            loadTracks,
            10,
            "sample.json",
            "",
            Collections.emptyList()
        );

        assertTargetOperationExistsInTask(
            "result",
            "write",
            tasks.getTargetsOperations(),
            loadTracks,
            2000,
            "Access All Arenas",
            "\"name\" : \"image\"",
            Arrays.asList(600L, 1L)
        );

        assertTargetOperationExistsInTask(
            pipelineName.contains("scala") ? "arg0" : "tracks",
            "read",
            tasks.getTargetsOperations(),
            countTracksByTrackName,
            2000,
            "Access All Arenas",
            "\"name\" : \"image\"",
            Arrays.asList(600L, 1L)
        );

        assertTargetOperationExistsInTask(
            "result",
            "write",
            tasks.getTargetsOperations(),
            countTracksByTrackName,
            700,
            "Radio Protector|    7",
            "\"name\" : \"count\"",
            Arrays.asList(407L, 2L)
        );

        assertTargetOperationExistsInTask(
            pipelineName.contains("scala") ? "arg0" : "tracks",
            "read",
            tasks.getTargetsOperations(),
            countTracksByArtist,
            2000,
            "No way back",
            "\"name\" : \"image\"",
            Arrays.asList(600L, 1L)
        );

        assertTargetOperationExistsInTask(
            "result",
            "write",
            tasks.getTargetsOperations(),
            countTracksByArtist,
            700,
            "65daysofstatic|  126",
            "\"name\" : \"count\"",
            Arrays.asList(36L, 2L)
        );

        if (pipelineName.contains("scala")) {
            assertMetricInTask(
                countTracksByTrackName,
                "additional_tracks_metric",
                2,
                "user"
            );

            if (verifySpark) {
                assertMetricInTask(
                    countTracksByTrackName,
                    "internal.metrics.input.recordsRead",
                    3,
                    "spark"
                );

                assertMetricInTask(
                    countTracksByArtist,
                    "internal.metrics.shuffle.write.recordsWritten",
                    36,
                    "spark"
                );
            } else {
                assertMetricNotInTask(countTracksByTrackName, "internal.metrics.input.recordsRead");
                assertMetricNotInTask(countTracksByArtist, "internal.metrics.shuffle.write.recordsWritten");
            }

            assertMetricInTask(
                countTracksByTrackName,
                "deequ.topTracks.*.Size",
                407.0,
                "user"
            );

            assertMetricInTask(
                countTracksByTrackName,
                "job_start_time",
                String.valueOf(Long.MAX_VALUE),
                "user"
            );

            assertMetricInTask(
                countTracksByArtist,
                "deequ.result.count is positive.Compliance",
                1.0,
                "user"
            );

            assertMetricInTask(
                countTracksByArtist,
                "deequ.result.count.Completeness",
                1.0,
                "user"
            );

            assertMetricInTask(
                countTracksByTrackName,
                "deequ.topTracks.count.Completeness",
                1.0,
                "user"
            );

            assertMetricInTask(
                countTracksByTrackName,
                "topTracks.count.min",
                1.0,
                "histograms"
            );

            assertMetricInTask(
                countTracksByTrackName,
                "topTracks.count.max",
                7.0,
                "histograms"
            );

            assertMetricInTask(
                countTracksByTrackName,
                "topTracks.count.mean",
                1.4742014742014742,
                "histograms"
            );
        }

        assertMetricInTask(
            countTracksByTrackName,
            "top_track_name",
            "Radio Protector",
            "user"
        );

        assertMetricInTask(
            countTracksByTrackName,
            "top_track_playcount",
            7,
            "user"
        );

        assertMetricInTask(
            countTracksByTrackName,
            "data.count.count",
            407,
            "histograms"
        );

        assertMetricInTask(
            countTracksByTrackName,
            "data.count.distinct",
            7,
            "histograms"
        );

        assertMetricInTask(
            countTracksByTrackName,
            "data.count.mean",
            1.4742014742014742,
            "histograms"
        );

        Summary nameSummary = new ColumnSummary(
            407,
            407,
            407,
            0,
            "string"
        );

        Summary countSummary = new NumericSummary(
            new ColumnSummary(
                407,
                7,
                407,
                0,
                "integer"
            ),
            7.0,
            1.4742014742014742,
            1.0,
            0.9008467766347343,
            1.0,
            1.0,
            2.0
        );

        Map<String, Object> nameSummaryMap = nameSummary.toMap();
        Map<String, Object> countSummaryMap = countSummary.toMap();

        Map<String, Map<String, Object>> stats = new HashMap<>(1);

        stats.put("name", nameSummaryMap);
        stats.put("count", countSummaryMap);

        assertMetricInTask(
            countTracksByTrackName,
            "data.stats",
            stats,
            "histograms"
        );

        assertLogsInTask(
            tasksAttemptsIds.get(loadTracks.getUid()),
            "Tracks was loaded from file "
        );

        assertLogsInTask(
            tasksAttemptsIds.get(countTracksByArtist.getUid()),
            "Completed counting top artists"
        );

        assertLogsInTask(
            tasksAttemptsIds.get(countTracksByTrackName.getUid()),
            "Track: Radio Protector with playcount: 7"
        );

        assertLogsInTask(
            tasksAttemptsIds.get(driverTask.getUid()),
            "Pipeline finished"
        );

        assertLogsInTask(
            tasksAttemptsIds.get(driverTask.getUid()),
            "Planning scan with bin packing, max size"
        );

        assertUpstreams(loadTracks, countTracksByArtist, graph, taskIds);
        assertUpstreams(loadTracks, countTracksByTrackName, graph, taskIds);

        TaskRun totalPlayCount = assertTaskExists("totalPlaycount", tasks, "failed");
        assertErrors(
            totalPlayCount,
            tasks,
            "Unable to count stuff",
            "java.lang.RuntimeException: Unable to count stuff",
            "java.lang.RuntimeException"
        );

        assertLogsInTask(
            tasksAttemptsIds.get(totalPlayCount.getUid()),
            "java.lang.RuntimeException: Unable to count stuff"
        );

        Response<MetricsForAlertsResponse> metricsForAlertsRes = api.metricsForAlerts(
                "[{\"name\":\"run_uid\",\"op\":\"eq\",\"val\":\"" + driverTask.getRunUid() + "\"}]")
            .execute();

        assertThat(String.format("Unable to get metrics for pipeline [%s]", jobName), metricsForAlertsRes.isSuccessful(), Matchers.is(true));

        MetricsForAlertsResponse metricsForAlerts = metricsForAlertsRes.body();
        assertThat(String.format("Numeric metrics are empty for pipeline [%s]", jobName), metricsForAlerts, Matchers.notNullValue());

        if (verifyAlerts) {
            assertMetricsAvailableForAlerts(
                jobName,
                "countTracksByArtist",
                "top_artist_playcount",
                metricsForAlerts
            );

            assertMetricsAvailableForAlerts(
                jobName,
                "countTracksByTrackName",
                "top_track_playcount",
                metricsForAlerts
            );
        }

        if (pipelineName.contains("scala")) {
            Map<String, List<DatasetOperationRes>> datasetByTask = fetchDatasetOperations(job);

            assertDatasetOperationExists(
                "loadTracks",
                "file:///broken/path",
                DatasetOperationType.WRITE,
                "FAILED",
                1,
                1,
                datasetByTask,
                "java.lang.RuntimeException",
                LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
            );

            assertDatasetOperationExists(
                "loadTracks",
                "",
                DatasetOperationType.READ,
                "SUCCESS",
                1,
                1,
                datasetByTask,
                LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
            );

            assertDatasetOperationExists(
                "loadTracks",
                "s3://datastore/sample2.json",
                DatasetOperationType.READ,
                "SUCCESS",
                1,
                1,
                datasetByTask,
                LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
            );

            assertDatasetOperationExists(
                "loadTracks",
                "s3://datastore/",
                DatasetOperationType.READ,
                "SUCCESS",
                1,
                1,
                datasetByTask,
                LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING
            );


            if (verifyDatasetOps) {
                assertDatasetOperationExists(
                    driverTask.getName(),
                    "resources",
                    DatasetOperationType.READ,
                    "SUCCESS",
                    3,
                    1,
                    datasetByTask,
                    LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
                );

                assertDatasetOperationExists(
                    "countTracksByArtist",
                    "resources",
                    DatasetOperationType.READ,
                    "SUCCESS",
                    9,
                    3,
                    datasetByTask,
                    LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
                );

                assertDatasetOperationExists(
                    "countTracksByTrackName",
                    "resources",
                    DatasetOperationType.READ,
                    "SUCCESS",
                    6,
                    2,
                    datasetByTask,
                    LogDataset.OP_SOURCE_SPARK_QUERY_LISTENER
                );
            }
        }
    }

    protected DatasetOperationRes assertDatasetOperationExists(String taskName,
                                                               String path,
                                                               DatasetOperationType type,
                                                               String status,
                                                               long records,
                                                               long operations,
                                                               Map<String, List<DatasetOperationRes>> datasets,
                                                               String operationSource) {
        return assertDatasetOperationExists(taskName, path, type, status, records, operations, datasets, null, operationSource);
    }

    /**
     * Assuming there is only one or two operations in each task.
     * Paths will be different on different environments and we can not rely on them.
     *
     * @param taskName
     * @param type
     * @param status
     * @param records
     * @param operations
     * @param datasets
     * @param error
     */
    protected DatasetOperationRes assertDatasetOperationExists(String taskName,
                                                               String path,
                                                               DatasetOperationType type,
                                                               String status,
                                                               long records,
                                                               long operations,
                                                               Map<String, List<DatasetOperationRes>> datasets,
                                                               String error,
                                                               String operationSource) {
        List<DatasetOperationRes> taskDatasets = datasets.get(taskName);
        Optional<DatasetOperationRes> datasetOpt = taskDatasets.stream().filter(datasetOperationRes -> datasetOperationRes.getOperationType().equalsIgnoreCase(type.toString()) && datasetOperationRes.getDatasetPath().contains(path)).findFirst();
        if (datasetOpt.isPresent()) {
            DatasetOperationRes dataset = datasetOpt.get();
            assertThat(String.format("Dataset operation is missing for task [%s]", taskName), dataset, Matchers.notNullValue());
            assertThat(String.format("Wrong dataset operation type for task [%s]", taskName), dataset.getOperationType(), Matchers.equalToIgnoringCase(type.toString()));
            assertThat(String.format("Wrong dataset operation status for task [%s]", taskName), dataset.getLatestOperationStatus(), Matchers.equalToIgnoringCase(status.toString()));
            assertThat(String.format("Wrong dataset operation records for task [%s]", taskName), dataset.getRecords(), Matchers.greaterThanOrEqualTo(records));
            assertThat(String.format("Wrong dataset operation operations for task [%s]", taskName), dataset.getOperations(), Matchers.greaterThanOrEqualTo(operations));
            if (error != null) {
                assertThat(String.format("Wrong dataset operation error for task [%s]", taskName), dataset.getIssues().get(0).getData().getOperationError(), Matchers.containsString(error));
            }
            assertThat(String.format("Wrong dataset operation source for task [%s]", taskName), dataset.getOperationSource(), Matchers.equalTo(operationSource));
            return dataset;
        } else {
            fail(String.format("Dataset operation of type [%s] with path [%s] for task [%s] not found", type.toString(), path, taskName));
            return null;
        }
    }

    protected TaskRun assertTaskExists(String taskName, Tasks tasks, String state) {
        Map<String, TaskRun> taskRuns = tasks.getTaskInstances()
            .values()
            .stream()
            .collect(Collectors.toMap(t -> t.getHasDownstreams() ? String.format("%s-parent", t.getTaskId()) : t.getTaskId(), Function.identity()));

        TaskRun task = taskRuns.get(taskName);

        assertThat(
            String.format(
                "Task [%s] should be created in dbnd but wasn't! There may be API compatibility issue. Existing tasks: %s",
                taskName,
                taskRuns.values().stream().map(t -> t.getHasDownstreams() ? String.format("%s-parent", t.getTaskId()) : t.getTaskId()).collect(Collectors.toList()).toString()
            ),
            task,
            Matchers.notNullValue()
        );

        assertThat(
            String.format("Wrong state for task [%s]", taskName),
            task.getState(),
            equalTo(state)
        );

        return task;
    }

    protected void assertTargetOperationExistsInTask(String paramName,
                                                     String operationType,
                                                     List<TargetOperation> targets,
                                                     TaskRun task,
                                                     int previewLength,
                                                     String content,
                                                     String schema,
                                                     List<Long> dimensions) {
        Optional<TaskRunParam> paramOpt = task.getTaskRunParams().stream()
            .filter(t -> t.getName().equals(paramName))
            .findAny();

        assertThat(
            String.format("Param [%s] not found in task [%s]", paramName, task.getTaskId()),
            paramOpt.isPresent(),
            equalTo(true)
        );

        List<String> targetOperationsUids = paramOpt.get().getTargetsUids();

        assertThat(
            String.format("Target operations should not be empty for param [%s] in task [%s]", paramName, task.getTaskId()),
            targetOperationsUids.isEmpty(),
            equalTo(false)
        );

        String targetUid = targetOperationsUids.iterator().next();

        Optional<TargetOperation> targetOpt = targets.stream()
            .filter(t -> operationType.equals(t.getOperationType()) && targetUid.equals(t.getTargetUid()))
            .findFirst();

        assertThat(
            String.format("Target [%s] with operation type [%s] not found in task [%s]", paramName, operationType, task.getTaskId()),
            targetOpt.isPresent(),
            equalTo(true)
        );

        TargetOperation target = targetOpt.get();

        assertThat("Value preview for target should not be empty",
            target.getValuePreview(),
            Matchers.notNullValue()
        );

        assertThat("Value preview for target should contain actual preview",
            target.getValuePreview().length(),
            Matchers.greaterThan(previewLength)
        );

        assertThat("Value preview should contain piece of content",
            target.getValuePreview(),
            Matchers.containsString(content)
        );

        assertThat("Data schema should contain piece of schema",
            target.getDataSchema(),
            Matchers.containsString(schema)
        );

        if (dimensions.isEmpty()) {
            return;
        }

        assertThat("Dimensions are wrong for target",
            target.getDataDimensions().toArray(),
            Matchers.array(equalTo(dimensions.get(0)), equalTo(dimensions.get(1)))
        );
    }

    protected void assertMetricNotInTask(TaskRun task, String key) throws IOException {
        TasksMetricsRequest req = new TasksMetricsRequest(Collections.singletonList(task.getUid()));

        Response<TasksMetricsResponse> res = api.tasksMetrics(req).execute();
        assertThat("Metrics response should be successful", res.isSuccessful(), equalTo(true));

        TasksMetricsResponse taskMetrics = res.body();
        assertThat("Metrics shouldn't be empty", taskMetrics, Matchers.notNullValue());

        Map<String, Map<String, List<List<Object>>>> metricsMap = taskMetrics.getMetrics();
        assertThat("Metrics shouldn't be empty", taskMetrics, Matchers.notNullValue());
        assertThat(String.format("There should be some metrics for task [%s]", task.getTaskId()), metricsMap.containsKey(task.getTaskId()));

        Map<String, List<List<Object>>> metricValues = metricsMap.get(task.getTaskId());
        assertThat(String.format("Metric [%s] does not exists in metrics response", key), metricValues.containsKey(key), equalTo(false));
    }

    protected void assertParamInTask(TaskRun task, String key, String value) {
        Optional<TaskRunParam> result = task.getTaskRunParams()
            .stream()
            .filter(taskRunParam -> key.equalsIgnoreCase(taskRunParam.getName()) && value.equalsIgnoreCase(taskRunParam.getValue()))
            .findAny();

        assertThat(String.format("Param [[%s]:[%s]] is missing in task %s", key, value, task.getName()), result.isPresent(), Matchers.equalTo(true));
    }

    protected void assertMetricInTask(TaskRun task, String key, Object value, String source) throws IOException {
        TasksMetricsRequest req = new TasksMetricsRequest(Collections.singletonList(task.getUid()));

        Response<TasksMetricsResponse> res = api.tasksMetrics(req).execute();
        assertThat("Metrics response should be successful", res.isSuccessful(), equalTo(true));

        TasksMetricsResponse taskMetrics = res.body();
        assertThat("Metrics shouldn't be empty", taskMetrics, Matchers.notNullValue());

        Map<String, Map<String, List<List<Object>>>> metricsMap = taskMetrics.getMetrics();
        assertThat("Metrics shouldn't be empty", taskMetrics, Matchers.notNullValue());
        assertThat(String.format("There should be some metrics for task [%s]", task.getTaskId()), metricsMap.containsKey(task.getTaskId()));

        Map<String, List<List<Object>>> metricValues = metricsMap.get(task.getTaskId());
        assertThat(String.format("Metric [%s] does not exists in metrics response", key), metricValues.containsKey(key), equalTo(true));

        List<List<Object>> metricValueAgg = metricValues.get(key);
        // TODO: deequ submits metrics several times and we get duplicated values
        if (!key.startsWith("deequ")) {
            assertThat("Metric value should contain one subarray", metricValueAgg.size(), equalTo(1));
        }

        List<Object> metricValue = metricValueAgg.get(0);
        assertThat("Metric value should contain three points: datetime, value, user", metricValue.size(), equalTo(3));
        if (value instanceof Map) {
            Map<String, Object> actualSummary = (Map<String, Object>) metricValue.get(1);
            Map<String, Object> exceptedSummary = (Map<String, Object>) value;

            for (Map.Entry<String, Object> summaryItem : exceptedSummary.entrySet()) {
                Map<String, Object> exceptedSummaryItem = (Map<String, Object>) summaryItem.getValue();

                assertThat(
                    "Summary item is missing",
                    actualSummary.containsKey(summaryItem.getKey()),
                    equalTo(true)
                );

                Map<String, Object> actualSummaryItem = (Map<String, Object>) actualSummary.get(summaryItem.getKey());

                for (Map.Entry<String, Object> summaryValue : exceptedSummaryItem.entrySet()) {
                    assertThat("Summary missing key", actualSummaryItem.containsKey(summaryValue.getKey()), equalTo(true));
                    // need to erase types because api returns integer and excepted values are longs
                    // compare doubles first
                    String exceptedValue = summaryValue.getValue().toString();
                    String actualValue = actualSummaryItem.get(summaryValue.getKey()).toString();
                    if (exceptedValue.contains(".") && actualValue.contains(".")) {
                        try {
                            double exceptedDouble = Double.parseDouble(exceptedValue);
                            Double actualDouble = Double.parseDouble(actualValue);
                            assertThat("Summary value is incorrect", actualDouble, closeTo(exceptedDouble, 0.000001));
                        } catch (NumberFormatException e) {
                            assertThat("Summary value is incorrect", actualValue, equalTo(exceptedValue));
                        }
                    } else {
                        assertThat("Summary value is incorrect", actualValue, equalTo(exceptedValue));
                    }
                }
            }

        } else if (value instanceof Double) {
            assertThat("Metric value (double) is incorrect", (Double) metricValue.get(1), closeTo((Double) value, 0.0001));
        } else {
            assertThat("Metric value is incorrect", metricValue.get(1), equalTo(value));
        }

        assertThat("Metric source is incorrect", metricValue.get(2), equalTo(source));
    }

    protected void assertProjectName(String rootRunUId, String projectName) throws IOException {
        Response<GetRunsResponse> res = api.runs(String.format("[{\"name\":\"uid\", \"op\":\"eq\", \"val\":\"%s\"}]", rootRunUId)).execute();
        assertThat("Runs response should be successful", res.isSuccessful(), equalTo(true));
        GetRunsResponse body = res.body();
        assertThat("Runs shouldn't be empty", body, Matchers.notNullValue());
        assertThat("Runs shouldn't be empty", body.getData(), Matchers.notNullValue());
        assertThat("Runs shouldn't be empty", body.getData().isEmpty(), Matchers.equalTo(false));
        Run next = body.getData().iterator().next();
        assertThat("Project name should be properly passed", next.getProjectName(), Matchers.equalTo(projectName));
    }

    protected void assertLogsInTask(Integer taskId, String logContent) throws IOException {
        Response<List<TaskRunAttemptLog>> res = api.logs(taskId).execute();
        assertThat("Logs response should be successful", res.isSuccessful(), equalTo(true));

        List<TaskRunAttemptLog> log = res.body();
        assertThat("Logs shouldn't be empty", log, Matchers.notNullValue());
        assertThat("Logs shouldn't be empty", log.isEmpty(), equalTo(false));

        TaskRunAttemptLog logBody = log.get(0);

        assertThat("Log body should have actual content", logBody.getLogBody(), Matchers.containsString(logContent));
    }

    protected void assertUpstreams(TaskRun upstream, TaskRun downstream, TaskFullGraph graph, Map<String, Integer> taskIds) {
        Integer upstreamId = taskIds.get(upstream.getUid());
        Integer downstreamId = taskIds.get(downstream.getUid());

        Optional<NodeRelationInfo> relOpt = graph.getUpstreams()
            .stream()
            .filter(n -> upstreamId.equals(n.getUpstreamTrId()) && downstreamId.equals(n.getDownstreamTrId()))
            .findAny();

        List<String> relationsAsString = graph.getChildren()
            .stream()
            .map(r -> String.format("%s -> %s", r.getUpstreamTrId(), r.getDownstreamTrId()))
            .collect(Collectors.toList());

        assertThat(
            String.format(
                "Upstream-downstream relation should exist between '%s' [%s] and '%s' [%s].\nExisting relations: %s",
                upstream.getTaskId(),
                upstreamId,
                downstream.getTaskId(),
                downstreamId,
                relationsAsString
            ),
            relOpt.isPresent(),
            equalTo(true)
        );
    }

    protected void assertErrors(TaskRun task, Tasks tasks, String errorText, String stackTrace, String errorType) {
        List<TaskRunAttempt> attempts = tasks.getAttempts().get(task.getUid());
        assertThat(String.format("Attempts is empty for task [%s]", task.getTaskId()), attempts.isEmpty(), equalTo(false));

        TaskRunAttempt attempt = attempts.iterator().next();
        ErrorInfo error = attempt.getError();
        assertThat(String.format("Error is missing for task [%s]", task.getTaskId()), error, Matchers.notNullValue());

        assertThat(String.format("Wrong error description for task [%s]", task.getTaskId()), error.getMsg(), Matchers.containsString(errorText));
        assertThat(String.format("Wrong error details for task [%s]", task.getTaskId()), error.getDatabandError(), equalTo(false));
        assertThat(String.format("Wrong error type for task [%s]", task.getTaskId()), error.getExcType(), Matchers.equalTo(errorType));
        assertThat(String.format("Wrong error details for task [%s]", task.getTaskId()), error.getTraceback(), Matchers.containsString(stackTrace));
    }

    protected void assertMetricsAvailableForAlerts(String jobName, String taskName, String metricName, MetricsForAlertsResponse res) {
        Optional<MetricForAlerts> metricOpt = res.getData().stream()
            .filter(m -> taskName.equals(m.getTaskName())
                && metricName.equals(m.getMetricName())
            ).findAny();

        assertThat(String.format("Metric [%s] for task [%s] does not exists", metricName, taskName), metricOpt.isPresent(), Matchers.is(true));
    }

    public Map<String, List<DatasetOperationRes>> fetchDatasetOperations(Job job) throws IOException {
        Response<List<DatasetOperationRes>> datasetsRes = api.operations(job.getLatestRunUid()).execute();
        List<DatasetOperationRes> datasets = datasetsRes.body();
        assertThat("Dataset operations shouldn't be empty", datasets, Matchers.notNullValue());
        assertThat("Dataset operations shouldn't be empty", datasets.isEmpty(), Matchers.equalTo(false));

        Map<String, List<DatasetOperationRes>> datasetByTask = new HashMap<>(1);
        for (DatasetOperationRes next : datasets) {
            datasetByTask.putIfAbsent(next.getTaskRunName(), new ArrayList<>(1));
            datasetByTask.get(next.getTaskRunName()).add(next);
        }
        return datasetByTask;
    }

    public void assertColumnStat(List<ColumnStats> columnsStats,
                                 String columnName,
                                 String columnType,
                                 long recordsCount,
                                 long distinctCount,
                                 double meanValue,
                                 double minValue,
                                 double maxValue,
                                 double stdValue,
                                 double quartile1,
                                 double quartile2,
                                 double quartile3) {
        Optional<ColumnStats> columnOpt = columnsStats.stream().filter(f -> f.getColumnName().equalsIgnoreCase(columnName)).findFirst();
        assertThat(String.format("Column stats are missing for column [%s]", columnName), columnOpt.isPresent(), Matchers.equalTo(true));
        ColumnStats col = columnOpt.get();
        assertThat("Wrong column type", col.getColumnType(), Matchers.equalTo(columnType));
        assertThat("Wrong records count", col.getRecordsCount(), Matchers.equalTo(recordsCount));
        assertThat("Wrong distinct count", col.getDistinctCount(), Matchers.equalTo(distinctCount));
        assertThat("Wrong mean", col.getMeanValue(), Matchers.closeTo(meanValue, 0.01));
        assertThat("Wrong max", col.getMaxValue(), Matchers.closeTo(maxValue, 0.01));
        assertThat("Wrong min", col.getMinValue(), Matchers.closeTo(minValue, 0.01));
        assertThat("Wrong std", col.getStdValue(), Matchers.closeTo(stdValue, 0.01));
        assertThat("Wrong 25%", col.getQuartile1(), Matchers.closeTo(quartile1, 0.01));
        assertThat("Wrong 50%", col.getQuartile2(), Matchers.closeTo(quartile2, 0.01));
        assertThat("Wrong 75%", col.getQuartile3(), Matchers.closeTo(quartile3, 0.01));
    }

}
