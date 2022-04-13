package ai.databand.examples;

import ai.databand.DbndApi;
import ai.databand.DbndClient;
import ai.databand.DbndPropertyNames;
import ai.databand.config.DbndConfig;
import ai.databand.config.DbndSparkConf;
import ai.databand.config.Env;
import ai.databand.config.JavaOpts;
import ai.databand.config.SimpleProps;
import ai.databand.schema.auth.CreateTokenReq;
import ai.databand.schema.auth.CreateTokenRes;
import ai.databand.schema.auth.LoginReq;
import ai.databand.schema.auth.LoginRes;
import okhttp3.Headers;
import org.hamcrest.Matchers;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;

public class ApiWithTokenBuilder {

    public DbndApi api() throws IOException {
        DbndConfig config = new DbndConfig();
        DbndClient dbnd = new DbndClient(config);
        DbndApi api = dbnd.api();

        Response<LoginRes> loginRes = api.login(new LoginReq()).execute();
        Headers headers = loginRes.headers();

        String cookie = Objects.requireNonNull(headers.get("set-cookie")).concat(";");

        Response<CreateTokenRes> tokenRes = api.createPersonalAccessToken(new CreateTokenReq(), cookie).execute();
        CreateTokenRes tokenResBody = tokenRes.body();
        assertThat("Token response body should not be empty", tokenResBody, Matchers.notNullValue());
        String token = tokenResBody.getToken();

        DbndConfig configWithToken = new DbndConfig(new DbndSparkConf(
            new Env(
                new JavaOpts(
                    new SimpleProps(
                        Collections.singletonMap(DbndPropertyNames.DBND__CORE__DATABAND_ACCESS_TOKEN, token)
                    )
                )
            )
        ));
        return new DbndClient(configWithToken).api();
    }

}
