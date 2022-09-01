/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand;

import ai.databand.schema.AddTaskRuns;
import ai.databand.schema.DatasetOperationRes;
import ai.databand.schema.GetRunsResponse;
import ai.databand.schema.InitRun;
import ai.databand.schema.Job;
import ai.databand.schema.LogDatasets;
import ai.databand.schema.LogMetric;
import ai.databand.schema.LogMetrics;
import ai.databand.schema.LogTargets;
import ai.databand.schema.MetricsForAlertsResponse;
import ai.databand.schema.PaginatedData;
import ai.databand.schema.SaveExternalLinks;
import ai.databand.schema.SaveTaskRunLog;
import ai.databand.schema.SetRunState;
import ai.databand.schema.TaskFullGraph;
import ai.databand.schema.TaskRunAttemptLog;
import ai.databand.schema.Tasks;
import ai.databand.schema.TasksMetricsRequest;
import ai.databand.schema.TasksMetricsResponse;
import ai.databand.schema.UpdateTaskRunAttempts;
import ai.databand.schema.auth.CreateTokenReq;
import ai.databand.schema.auth.CreateTokenRes;
import ai.databand.schema.auth.LoginReq;
import ai.databand.schema.auth.LoginRes;
import ai.databand.schema.tasks.GetTasksReq;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Header;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.util.List;

public interface DbndApi {

    @POST("/api/v1/tracking/init_run")
    Call<Void> initRun(@Body InitRun data);

    @POST("/api/v1/tracking/add_task_runs")
    Call<Void> addTaskRuns(@Body AddTaskRuns data);

    @POST("/api/v1/tracking/log_metric")
    Call<Void> logMetric(@Body LogMetric data);

    @POST("/api/v1/tracking/log_metrics")
    Call<Void> logMetrics(@Body LogMetrics data);

    @POST("/api/v1/tracking/save_task_run_log")
    Call<Void> saveTaskRunLog(@Body SaveTaskRunLog data);

    @POST("/api/v1/tracking/log_targets")
    Call<Void> logTargets(@Body LogTargets data);

    @POST("/api/v1/tracking/log_datasets")
    Call<Void> logDatasets(@Body LogDatasets data);

    @POST("/api/v1/tracking/set_run_state")
    Call<Void> setRunState(@Body SetRunState data);

    @POST("/api/v1/tracking/update_task_run_attempts")
    Call<Void> updateTaskRunAttempts(@Body UpdateTaskRunAttempts data);

    @POST("/api/v1/tracking/save_external_links")
    Call<Void> saveExternalLinks(@Body SaveExternalLinks data);

    @POST("/api/v1/auth/login")
    Call<LoginRes> login(@Body LoginReq data);

    @GET("/api/v1/task/full-graph")
    Call<TaskFullGraph> taskFullGraph(@Query("job_name") String jobName, @Query("run_uid") String runUid);

    @GET("/api/v1/runs")
    Call<GetRunsResponse> runs(@Query("filter") String filter);

    @POST("/api/v1/task/tasks-metrics")
    Call<TasksMetricsResponse> tasksMetrics(@Body TasksMetricsRequest data);

    @GET("/api/v1/jobs")
    Call<PaginatedData<Job>> jobs(@Query("sort") String filter);

    @POST("/api/v1/task/tasks")
    Call<Tasks> tasks(@Body GetTasksReq taskUids);

    @GET("/api/v1/task/tasks-logs")
    Call<List<TaskRunAttemptLog>> logs(@Query("attempt_id") Integer taskRunAttemptId);

    @GET("/api/v1/metrics/for_alerts")
    Call<MetricsForAlertsResponse> metricsForAlerts(@Query("filter") String filter);

    @GET("/api/v1/runs/{run_uid}/operations")
    Call<List<DatasetOperationRes>> operations(@Path("run_uid") String runUid);

    @POST("/api/v1/auth/personal_access_token")
    Call<CreateTokenRes> createPersonalAccessToken(@Body CreateTokenReq req, @Header("Cookie") String cookie);

}
