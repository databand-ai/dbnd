/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand;

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
import retrofit2.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * This api builder is used in unit tests.
 * It has to be placed here, because okhttp3 packages has to be relocated to avoid conflicts with Spark distributions.
 */
public class ApiWithTokenBuilder {

    public DbndApi api() throws IOException {
        DbndConfig config = new DbndConfig();
        DbndClient dbnd = new DbndClient(config);
        DbndApi api = dbnd.api();

        Response<Void> csrfRes = api.csrfToken().execute();
        Optional<String> optionalCookie = csrfRes.headers().values("set-cookie").stream().filter(cookieStr -> cookieStr.contains("X-CSRF-TOKEN")).findFirst();
        String csrfToken = optionalCookie.orElseThrow(NullPointerException::new).split(";")[0].split("=")[1];

        String cookie = Objects.requireNonNull(csrfRes.headers().get("set-cookie")).concat(";");

        Response<LoginRes> loginRes = api.login(new LoginReq(), cookie, csrfToken).execute();
        Headers headers = loginRes.headers();

        cookie = Objects.requireNonNull(headers.get("set-cookie")).concat(";");

        Response<CreateTokenRes> tokenRes = api.createPersonalAccessToken(new CreateTokenReq(), cookie, csrfToken).execute();
        CreateTokenRes tokenResBody = tokenRes.body();
        Objects.requireNonNull(tokenResBody, "Token response body should not be empty");
        String token = tokenResBody.getToken();

        String finalCookie = cookie;
        Map<String, String> tokens = new HashMap<String, String>() {{
            put(DbndPropertyNames.DBND__CORE__DATABAND_ACCESS_TOKEN, token);
            put(DbndPropertyNames.DBND__CSRF_TOKEN, csrfToken);
            put(DbndPropertyNames.DBND__SESSION_COOKIE, finalCookie);
        }};

        DbndConfig configWithToken = new DbndConfig(new DbndSparkConf(
            new Env(
                new JavaOpts(
                    new SimpleProps(tokens)
                )
            )
        ));
        return new DbndClient(configWithToken).api();
    }

}
