package ai.databand.azkaban;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Stateful client
 */
public class AzkabanClient {

    private final AzkabanApi api;

    public AzkabanClient(String azkabanUrl) {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
            .readTimeout(60, TimeUnit.SECONDS)
            .connectTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true);

        api = new Retrofit.Builder()
            .client(clientBuilder.build())
            .baseUrl(azkabanUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .build()
            .create(AzkabanApi.class);
    }

    public Optional<String> login(String username, String password) {
        try {
            Response<LoginRes> res = api.login(username, password).execute();
            LoginRes body = res.body();
            if (body != null) {
                return Optional.of(body.getSessionId());
            }
            return Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }


    public Optional<String> createProject(String sessionId, String name, String description) {
        try {
            Response<CreateProjectRes> res = api.createProject(sessionId, name, description).execute();
            CreateProjectRes body = res.body();
            if (body != null && body.isSuccess()) {
                return Optional.of(name);
            }
            return Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public boolean uploadProject(String sessionId, String project, byte[] fileContent) {
        RequestBody file = MultipartBody.create(MediaType.parse("application/zip"), fileContent);
        MultipartBody.Part filePart = MultipartBody.Part.createFormData("file", "project.zip", file);
        MultipartBody.Part projectNamePart = MultipartBody.Part.createFormData("project", project);
        MultipartBody.Part sessionIdPart = MultipartBody.Part.createFormData("session.id", sessionId);
        MultipartBody.Part action = MultipartBody.Part.createFormData("ajax", "upload");

        try {
            Response<UploadProjectRes> res = api.uploadProject(action, sessionIdPart, projectNamePart, filePart).execute();
            UploadProjectRes body = res.body();
            return body != null && body.getError() == null;
        } catch (IOException e) {
            return false;
        }
    }

    public Optional<Integer> executeFlow(String sessionId, String project, String flow) {
        try {
            Response<ExecuteFlowRes> res = api.executeFlow(sessionId, project, flow).execute();
            ExecuteFlowRes body = res.body();
            if (body == null || !body.isSuccess()) {
                return Optional.empty();
            }
            return Optional.of(body.getExecId());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public Optional<FetchFlowExecutionRes> fetchFlowExecution(String sessionId, Integer executionId) {
        try {
            Response<FetchFlowExecutionRes> res = api.fetchFlowExecution(sessionId, executionId).execute();
            FetchFlowExecutionRes body = res.body();
            if (body == null || !body.isSuccess()) {
                return Optional.empty();
            }
            return Optional.of(body);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

}
