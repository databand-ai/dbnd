/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import ai.databand.config.DbndConfig;
import ai.databand.config.SimpleProps;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_HEAD_BYTES;
import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_TAIL_BYTES;
import static ai.databand.log.TruncatedLog.EMPTY_LOG_MSG;
import static ai.databand.log.TruncatedLog.PLACEHOLDER;
import static org.hamcrest.MatcherAssert.assertThat;

class SaveTaskRunLogTest {

    @Test
    public void testHeadTailZero() {
        Map<String, String> props = new HashMap<>(1);
        props.put(DBND__LOG__PREVIEW_HEAD_BYTES, "0");
        props.put(DBND__LOG__PREVIEW_TAIL_BYTES, "0");

        DbndConfig config = new DbndConfig(new SimpleProps(props));

        String logBody = "This is 20 bytes log";

        SaveTaskRunLog log = new SaveTaskRunLog(config, "1", logBody);

        assertThat("Log should be full when it's less than limits", log.getLogBody(), Matchers.equalTo(EMPTY_LOG_MSG));
    }
    @Test
    public void testHeadTailDefaultZero() {
        Map<String, String> props = new HashMap<>(1);

        DbndConfig config = new DbndConfig(new SimpleProps(props));

        String logBody = "This is 20 bytes log";

        SaveTaskRunLog log = new SaveTaskRunLog(config, "1", logBody);

        assertThat("Log should be full when it's less than limits", log.getLogBody(), Matchers.equalTo(EMPTY_LOG_MSG));
    }

    @Test
    public void testTruncateLessThanLimits() {
        Map<String, String> props = new HashMap<>(1);
        props.put(DBND__LOG__PREVIEW_HEAD_BYTES, "160");
        props.put(DBND__LOG__PREVIEW_TAIL_BYTES, "160");

        DbndConfig config = new DbndConfig(new SimpleProps(props));

        String logBody = "This is 20 bytes log";

        SaveTaskRunLog log = new SaveTaskRunLog(config, "1", logBody);

        assertThat("Log should be full when it's less than limits", log.getLogBody(), Matchers.equalTo(logBody));
    }

    @Test
    public void testTruncateHeadTail() {
        Map<String, String> props = new HashMap<>(1);
        props.put(DBND__LOG__PREVIEW_HEAD_BYTES, "16");
        props.put(DBND__LOG__PREVIEW_TAIL_BYTES, "16");

        DbndConfig config = new DbndConfig(new SimpleProps(props));

        String logBody = "This is log where space between head and tail should be truncated";

        SaveTaskRunLog log = new SaveTaskRunLog(config, "1", logBody);

        String headStr = "This is log wher";
        String tailStr = "uld be truncated";

        String truncated = headStr + String.format(PLACEHOLDER, 16, 16) + tailStr;

        assertThat("Log should be full when it's less than limits", log.getLogBody(), Matchers.equalTo(truncated));
    }



}
