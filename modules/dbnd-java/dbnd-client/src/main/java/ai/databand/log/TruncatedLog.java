package ai.databand.log;

import ai.databand.config.DbndConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TruncatedLog {

    private static final Logger LOG = LoggerFactory.getLogger(TruncatedLog.class);

    public static final String EMPTY_LOG_MSG = "Log truncated to empty";
    public static final String EMPTY_ERROR_LOG_MSG = "Error occurred during reading log file";
    public static final String PLACEHOLDER = "\r\n...\r\n\r\nThe log body is truncated by databand, fetched %s bytes for the `head` and %s bytes for the `tail` from the whole {file_size} bytes of the file.\\r\\nControl the log preview length with dbnd.log.preview_head_bytes and dbnd.log.preview_tail_bytes\r\n\r\n...\r\n";

    private String log;

    public TruncatedLog(DbndConfig config, String logBody) {
        int headBytes = config.previewHeadBytes();
        int tailBytes = config.previewTailBytes();

        byte[] logBytes = logBody.getBytes();

        if (headBytes == 0 && tailBytes == 0) {
            // truncate to zero
            this.log = EMPTY_LOG_MSG;
            return;
        }

        if (headBytes + tailBytes >= logBytes.length) {
            // do not truncate at all
            this.log = logBody;
        } else {
            // fetch head + tail
            String headStr = new String(Arrays.copyOfRange(logBytes, 0, headBytes));
            String tailStr = new String(Arrays.copyOfRange(logBytes, logBytes.length - tailBytes, logBytes.length));
            this.log = headStr + String.format(PLACEHOLDER, headBytes, tailBytes) + tailStr;
        }
    }

    public TruncatedLog(DbndConfig config, File logFile) {
        int headBytes = config.previewHeadBytes();
        int tailBytes = config.previewTailBytes();
        try {
            if (logFile.length() <= config.previewTotalBytes()) {
                try (Stream<String> lines = Files.lines(logFile.toPath())) {
                    this.log = lines.collect(Collectors.joining("\n"));
                }
            } else {
                // todo: potential OOM in case of very large limits
                byte[] head = new byte[headBytes];
                byte[] tail = new byte[tailBytes];

                try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
                    raf.read(head, 0, headBytes);

                    raf.seek(logFile.length() - tailBytes);
                    raf.read(tail, 0, tailBytes);

                    this.log = new String(head) + String.format(PLACEHOLDER, headBytes, tailBytes) + new String(tail);
                }
            }
        } catch (IOException e) {
            LOG.error(String.format("Unable to read log file %s", logFile.getAbsolutePath()), e);
            this.log = EMPTY_ERROR_LOG_MSG;
        }
    }

    public String toString() {
        return log;
    }

}
