package ai.databand;

import ai.databand.config.DbndConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DbndApiBuilder {

    private final DbndConfig config;

    public DbndApiBuilder(DbndConfig config) {
        this.config = config;
    }

    public DbndApi build() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        } catch (java.lang.NoSuchFieldError e) {
            // jackson 2.6 used
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        }

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
            .readTimeout(60, TimeUnit.SECONDS)
            .connectTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            // we will handle redirects manually
            .followRedirects(false)
            .followSslRedirects(false)
            // enforce HTTP 1 to avoid threads hanging, see https://github.com/square/okhttp/issues/4029
            .protocols(Collections.singletonList(Protocol.HTTP_1_1))
            .retryOnConnectionFailure(true);

        /*
         * If personal access token is enabled then we will add corresponding header into each API call.
         * Access token should be passed in "Authorization" header.
         */
        if (config.personalAccessToken().isPresent()) {
            clientBuilder.addInterceptor(
                chain -> {
                    Request origin = chain.request();
                    Request withAuth = origin
                        .newBuilder()
                        .addHeader("Authorization", String.format("Bearer %s", config.personalAccessToken().get()))
                        .build();
                    return chain.proceed(withAuth);
                }
            );
        }
        /*
         * OkHttp doesn't do proper redirects on 301: https://github.com/square/okhttp/issues/6627
         * This interceptor introduces workaround — if request is being redirected we will handle it manually.
         */
        clientBuilder.addInterceptor(
            chain -> {
                Request origin = chain.request();
                Response response = chain.proceed(origin);
                if (!response.isRedirect()) {
                    return response;
                }
                String newLocation = response.header("Location");
                if (newLocation == null) {
                    return response;
                }
                Request withNewLocation = origin
                    .newBuilder()
                    .url(newLocation)
                    .build();
                return chain.proceed(withNewLocation);
            }
        );

//        disabled until we'll figure out way to upgrade okio library
//        if (config.isVerbose()) {
//            HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor();
//            loggingInterceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
//            clientBuilder.addInterceptor(loggingInterceptor);
//        }

        OkHttpClient client = clientBuilder.build();

        Retrofit.Builder builder = new Retrofit.Builder()
            .client(client)
            .baseUrl(config.databandUrl())
            .addConverterFactory(JacksonConverterFactory.create(objectMapper));

        return builder.build().create(DbndApi.class);
    }

}
