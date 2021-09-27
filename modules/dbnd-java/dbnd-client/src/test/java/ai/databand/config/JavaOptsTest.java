package ai.databand.config;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.DBND__CORE__DATABAND_URL;
import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_HEAD_BYTES;
import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_TAIL_BYTES;
import static ai.databand.DbndPropertyNames.DBND__TRACKING__VERBOSE;
import static org.hamcrest.MatcherAssert.assertThat;

class JavaOptsTest {

    @Test
    public void testArgsParse() {
        Map<String, String> systemProps = new HashMap<>(1);
        systemProps.put("dbnd.tracking.verbose", "True");
        systemProps.put("dbnd.core.databand_url", "https://tracker.databand.ai");
        systemProps.put("dbnd.log.preview_head_bytes", "100");
        systemProps.put("dbnd.log.preview_tail_bytes", " 555 ");

        JavaOpts opts = new JavaOpts(systemProps);
        Map<String, String> values = opts.values();

        assertThat("Key is missing, but should exist", values.containsKey(DBND__TRACKING__VERBOSE), Matchers.equalTo(true));
        assertThat("Wrong value", values.get(DBND__TRACKING__VERBOSE), Matchers.equalTo("True"));
        assertThat("Wrong value", values.get(DBND__CORE__DATABAND_URL), Matchers.equalTo("https://tracker.databand.ai"));
        assertThat("Wrong value", values.get(DBND__LOG__PREVIEW_HEAD_BYTES), Matchers.equalTo("100"));
        assertThat("Wrong value", values.get(DBND__LOG__PREVIEW_TAIL_BYTES), Matchers.equalTo("555"));

        assertThat("Key is present but shouldn't", values.size(), Matchers.equalTo(4));
    }

}
