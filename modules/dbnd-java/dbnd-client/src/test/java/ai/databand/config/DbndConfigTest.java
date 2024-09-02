/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.config;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_DAG_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_EXECUTION_DATE;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TASK_ID;
import static ai.databand.DbndPropertyNames.AIRFLOW_CTX_TRY_NUMBER;
import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_ACCESS_TOKEN;
import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_URL;
import static ai.databand.DbndPropertyNames.DBND__TRACKING;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__LOG_VALUE_PREVIEW;
import static org.hamcrest.MatcherAssert.assertThat;

class DbndConfigTest {

    private final Properties sparkProperties = System.getProperties();

    @Test
    public void testMaskValue() {
        DbndConfig conf = new DbndConfig();
        assertThat("Sensitive value should be masked", conf.maskValue("dbnd.core.databand_access_token", "token"), Matchers.equalTo("***"));
        assertThat("Null value should not be changed", conf.maskValue("dbnd.core.databand_access_token", null), Matchers.nullValue());
        assertThat("Non-sensitive value should not be changed", conf.maskValue("dbnd.core.databand_url", "https://databand.ai/tracker/"), Matchers.equalTo("https://databand.ai/tracker/"));
    }

    @Test
    public void testToString() {
        DbndConfig conf = new DbndConfig(new SimpleProps(Collections.emptyMap()));
        assertThat("Empty config should return nothing", conf.toString(), Matchers.equalTo("{}"));

        Map<String, String> props = new LinkedHashMap<>(1);
        props.put(DBND__TRACKING, "True");
        props.put("SOME_VAR", "False");
        props.put(DBND__CORE__DATABAND_URL, "https://databand.ai/tracker");
        props.put(DBND__CORE__DATABAND_ACCESS_TOKEN, "123");
        conf = new DbndConfig(new SimpleProps(props));
        String excepted = "\ndbnd.tracking=True\ndbnd.core.databand_url=https://databand.ai/tracker\ndbnd.core.databand_access_token=***";
        assertThat("Wrong toString", conf.toString(), Matchers.equalTo(excepted));
    }

    @Test
    public void testTrackingEnabledNonAf() {
        checkTrackingEnabled(
            "Tracking should be enabled when env variable DBND__TRACKING set to True and we're running not in airflow",
            Collections.singletonMap(DBND__TRACKING, "True"),
            true
        );

        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING set to False",
            Collections.singletonMap(DBND__TRACKING, "False"),
            false
        );

        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING is empty and we're running not in airflow context",
            Collections.emptyMap(),
            false
        );

        sparkProperties.put("spark.env.DBND__TRACKING", "True");
        checkTrackingEnabled(
            "Tracking should be enabled when spark conf variable DBND__TRACKING set to True and we're running not in airflow",
            Collections.singletonMap(DBND__TRACKING, "False"),
            true
        );

        sparkProperties.put("spark.env.DBND__TRACKING", "False");

        checkTrackingEnabled(
            "Tracking should be disabled when spark conf variable DBND__TRACKING set to False",
            Collections.singletonMap(DBND__TRACKING, "True"),
            false
        );
        sparkProperties.remove("spark.env.DBND__TRACKING");


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
            afContext,
            true
        );

        Map<String, String> afContextWithDisabledTracking = new HashMap<>(afContext);
        afContextWithDisabledTracking.put(DBND__TRACKING, "False");
        checkTrackingEnabled(
            "Tracking should be disabled when env variable DBND__TRACKING set to False and we're running in airflow",
            afContextWithDisabledTracking,
            false
        );
    }

    protected void checkTrackingEnabled(String reason, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new DbndSparkConf(new SimpleProps(env)));
        assertThat(reason, conf.isTrackingEnabled(), Matchers.equalTo(expectedValue));
    }

    @Test
    public void testPreviewEnabled() {
        sparkProperties.put("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "True");

        checkPreviewEnabled(
            "Preview should be enabled when env variable DBND__TRACKING__LOG_VALUE_PREVIEW set to True",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "True"),
            true
        );
        checkPreviewEnabled(
            "Preview should be enabled when spark conf variable DBND__TRACKING__LOG_VALUE_PREVIEW set to True",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "False"),
            true
        );

        sparkProperties.put("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "False");

        checkPreviewEnabled(
            "Preview should be disabled when env variable DBND__TRACKING__LOG_VALUE_PREVIEW set to False",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "False"),
            false
        );

        sparkProperties.put("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "False");

        checkPreviewEnabled(
            "Preview should be disabled when spark conf variable DBND__TRACKING__LOG_VALUE_PREVIEW set to False",
            Collections.singletonMap(DBND__TRACKING__LOG_VALUE_PREVIEW, "True"),
            false
        );
        sparkProperties.remove("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW");
    }

    protected void checkPreviewEnabled(String reason, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new DbndSparkConf(new SimpleProps(env)));
        assertThat(reason, conf.isPreviewEnabled(), Matchers.equalTo(expectedValue));
    }

    @Test
    public void testPipelineRunName() {
        sparkProperties.put("spark.env.DBND__RUN_INFO__NAME", "pipeline_run");
        sparkProperties.put("spark.env.AIRFLOW_CTX_TRY_NUMBER", "1");
        DbndConfig conf = new DbndConfig(new DbndSparkConf(new SimpleProps()));
        assertThat("Wrong pipeline run name", conf.runName(), Matchers.equalTo("pipeline_run"));
        sparkProperties.remove("spark.env.DBND__RUN_INFO__NAME");
        sparkProperties.remove("spark.env.AIRFLOW_CTX_TRY_NUMBER");

    }

    @Test
    public void testDatabandUrl() {
        checkDatabandUrl(
            "Databand URL should be set to http://localhost:8080 when no env variable is present",
            Collections.emptyMap(),
            "http://localhost:8080"
        );

        sparkProperties.put("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW", "False");

        checkDatabandUrl(
            "Databand URL should be read form env variables",
            Collections.singletonMap(DBND__CORE__DATABAND_URL, "https://tracker.databand.ai"),
            "https://tracker.databand.ai"
        );


        sparkProperties.put("spark.env.DBND__CORE__DATABAND_URL", "https://tracker2.databand.ai");

        checkDatabandUrl(
            "Databand URL should be read form spark conf variables when no env variable is present",
            Collections.singletonMap(DBND__CORE__DATABAND_URL, "https://tracker.databand.ai"),
            "https://tracker2.databand.ai"
        );
        sparkProperties.remove("spark.env.DBND__TRACKING__LOG_VALUE_PREVIEW");
        sparkProperties.remove("spark.env.DBND__CORE__DATABAND_URL");

    }

    // Databricks Notebook name is available at runtime using a second Spark Listener
    @Test
    public void testDynamicSparkProperties() {
        // no info about job type nor path
        DbndConfig conf = new DbndConfig();
        conf.setSparkProperties(sparkProperties);
        assertThat("Spark app name should be undefined without Spark", conf.sparkAppName(), Matchers.equalTo("none"));

        // correct job type, but no info about path
        sparkProperties.put("spark.databricks.job.type", "notebook");
        conf = new DbndConfig();
        conf.setSparkProperties(sparkProperties);
        assertThat("Spark app name should be undefined without Spark", conf.sparkAppName(), Matchers.equalTo("none"));

        // correct job type and correct Notebook path
        String notebookName = "my_notebook_name";
        String notebookPath = "/Users/my.user@ibm.com/" + notebookName;
        sparkProperties.put("spark.databricks.notebook.path", notebookPath);
        conf = new DbndConfig();
        conf.setSparkProperties(sparkProperties);
        assertThat(String.format("Spark app name should be detected and set to \"%s\"", notebookName), conf.sparkAppName(), Matchers.equalTo(notebookName));

        // correct job type and correct python script path
        String pythonScriptName = "myscript.py";
        String pythonScriptPath = "/dbfs:/FileStore/job-jars/mysuser/" + pythonScriptName;
        sparkProperties.put("spark.databricks.notebook.path", pythonScriptPath);
        sparkProperties.put("spark.databricks.job.type", "python");
        conf = new DbndConfig();
        conf.setSparkProperties(sparkProperties);
        assertThat(String.format("Spark app name should be detected and set to \"%s\"", pythonScriptName), conf.sparkAppName(), Matchers.equalTo(pythonScriptName));
    }

    protected void checkDatabandUrl(String reason, Map<String, String> env, Object expectedValue) {
        DbndConfig conf = new DbndConfig(new DbndSparkConf(new SimpleProps(env)));
        assertThat(reason, conf.databandUrl(), Matchers.equalTo(expectedValue));
    }

}
