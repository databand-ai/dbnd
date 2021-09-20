package ai.databand.log;

import ai.databand.config.DbndConfig;
import ai.databand.config.SimpleProps;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_HEAD_BYTES;
import static ai.databand.DbndPropertyNames.DBND__LOG__PREVIEW_TAIL_BYTES;
import static ai.databand.log.TruncatedLog.PLACEHOLDER;
import static org.hamcrest.MatcherAssert.assertThat;

class TruncatedLogTest {

    @Test
    public void testFileTruncate() {
        Map<String, String> props = new HashMap<>(1);
        props.put(DBND__LOG__PREVIEW_HEAD_BYTES, "16");
        props.put(DBND__LOG__PREVIEW_TAIL_BYTES, "16");

        DbndConfig config = new DbndConfig(new SimpleProps(props));

        File logFile = new File(TruncatedLogTest.class.getClassLoader().getResource("logfile.txt").getFile());

        TruncatedLog log = new TruncatedLog(config, logFile);

        String headStr = "[2020-12-23 15:2";
        String tailStr = "3T13:26:42.674Z\n";

        String truncated = headStr + String.format(PLACEHOLDER, 16, 16) + tailStr;

        assertThat("Wrong file truncate", log.toString(), Matchers.equalTo(truncated));
    }

}
