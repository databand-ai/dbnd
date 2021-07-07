package ai.databand;

import ai.databand.config.DbndConfig;
import ai.databand.config.SimpleProps;
import ai.databand.config.SparkConf;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_DAG_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_EXECUTION_DATE;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TASK_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TRY_NUMBER;
import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_URL;
import static ai.databand.DbndPropertyNames.DBND__TRACKING;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__LOG_VALUE_PREVIEW;
import static org.hamcrest.MatcherAssert.assertThat;

class DbndConfigTest {

    @Test
    public void testTrackingEnabledNonAf() {
        checkTrackingEnabled(
            "Tracking should be enabled when env variable DBND__TRACKING set to True and we're running not in airflow",
            "",
            Collections.singletonMap(DBND__TRACKING, "True"),
            true
        );

        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING set to False",
            "",
            Collections.singletonMap(DBND__TRACKING, "False"),
            false
        );

        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING is empty and we're running not in airflow context",
            "",
            Collections.emptyMap(),
            false
        );

        checkTrackingEnabled(
            "Tracking should be enabled when spark conf variable DBND__TRACKING set to True and we're running not in airflow",
            "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__TRACKING=True",
            Collections.singletonMap(DBND__TRACKING, "False"),
            true
        );
        checkTrackingEnabled(
            "Tracking should be disabled when spark conf variable DBND__TRACKING set to False",
            "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__TRACKING=False",
            Collections.singletonMap(DBND__TRACKING, "True"),
            false
        );
    }

    @Test
    public void testTrackingEnabledAf() {
        Map<String, String> afContext = new HashMap<>(1);
        afContext.put(AIRFLOW_CTX_DAG_ID, "process_data");
        afContext.put(AIRFLOW_CTX_EXECUTION_DATE, "2020-02-02");
        afContext.put(AIRFLOW_CTX_TASK_ID, "run");
        afContext.put(AIRFLOW_CTX_TRY_NUMBER, "1");

        checkTrackingEnabled(
            "Tracking should be enabled when env variable DBND__TRACKING is not set to True and we're running in airflow",
            "",
            afContext,
            true
        );

        Map<String, String> afContextWithDisabledTracking = new HashMap<>(afContext);
        afContextWithDisabledTracking.put(DBND__TRACKING, "False");
        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING set to False and we're running in airflow",
            "",
            afContextWithDisabledTracking,
            false
        );
    }

    protected void checkTrackingEnabled(String reason, String cmd, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(env), cmd), cmd);
        assertThat(reason, conf.isTrackingEnabled(), Matchers.equalTo(expectedValue));
    }

    @Test
    public void testPreviewEnabled() {
        checkPreviewEnabled(
            "Preview should be enabled when env variable DBND__TRACKING__LOG_VALUE_PREVIEW set to True",
            "",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "True"),
            true
        );

        checkPreviewEnabled(
            "Preview should be enabled when spark conf variable DBND__TRACKING__LOG_VALUE_PREVIEW set to True",
            "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW=True",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "False"),
            true
        );

        checkPreviewEnabled(
            "Preview should be disabled when env variable DBND__TRACKING__LOG_VALUE_PREVIEW set to False",
            "",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "False"),
            false
        );

        checkPreviewEnabled(
            "Preview should be disabled when spark conf variable DBND__TRACKING__LOG_VALUE_PREVIEW set to False",
            "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW=False",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "True"),
            false
        );
    }

    protected void checkPreviewEnabled(String reason, String cmd, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(env), cmd), cmd);
        assertThat(reason, conf.isPreviewEnabled(), Matchers.equalTo(expectedValue));
    }

    @Test
    public void testPipelineRunName() {
        String cmd = "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__RUN__NAME=pipeline_run";
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(), cmd), cmd);
        assertThat("Wrong pipeline run name", conf.runName(), Matchers.equalTo("pipeline_run"));
    }

    @Test
    public void testDatabandUrl() {
        checkDatabandUrl(
            "Databand URL should be set to http://localhost:8080 when no env variable is present",
            "",
            Collections.emptyMap(),
            "http://localhost:8080"
        );

        checkDatabandUrl(
            "Databand URL should be read form env variables",
            "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW=False",
            Collections.singletonMap(DBND__CORE__DATABAND_URL, "https://tracker.databand.ai"),
            "https://tracker.databand.ai"
        );

        checkDatabandUrl(
            "Databand URL should be read form spark conf variables when no env variable is present",
            "org.apache.spark.deploy.SparkSubmit --master local --conf spark.env.DBND__CORE__DATABAND_URL=https://tracker2.databand.ai",
            Collections.singletonMap(DBND__CORE__DATABAND_URL, "https://tracker.databand.ai"),
            "https://tracker2.databand.ai"
        );
    }

    protected void checkDatabandUrl(String reason, String cmd, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new SparkConf(new SimpleProps(env), cmd), cmd);
        assertThat(reason, conf.databandUrl(), Matchers.equalTo(expectedValue));
    }

}
