/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.azkaban;

import okhttp3.MultipartBody;
import retrofit2.Call;
import retrofit2.http.Field;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;

public interface AzkabanApi {

    @FormUrlEncoded
    @POST("/?action=login")
    Call<LoginRes> login(@Field("username") String username, @Field("password") String password);

    @FormUrlEncoded
    @POST("/manager?action=create")
    Call<CreateProjectRes> createProject(@Field("session.id") String sessionId,
                                         @Field("name") String name,
                                         @Field("description") String description);

    @Multipart
    @POST("/manager")
    Call<UploadProjectRes> uploadProject(@Part MultipartBody.Part action,
                                         @Part MultipartBody.Part sessionId,
                                         @Part MultipartBody.Part project,
                                         @Part MultipartBody.Part file);

    @FormUrlEncoded
    @POST("/executor?ajax=executeFlow")
    Call<ExecuteFlowRes> executeFlow(@Field("session.id") String sessionId,
                                     @Field("project") String project,
                                     @Field("flow") String flow);

    @FormUrlEncoded
    @POST("/executor?ajax=fetchexecflow")
    Call<FetchFlowExecutionRes> fetchFlowExecution(@Field("session.id") String sessionId,
                                                   @Field("execid") Integer executionId);

}
