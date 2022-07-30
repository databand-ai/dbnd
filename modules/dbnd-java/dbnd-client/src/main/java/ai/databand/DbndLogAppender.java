/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class DbndLogAppender extends AppenderSkeleton {

    private final DbndWrapper dbndWrapper;

    public DbndLogAppender(DbndWrapper dbndWrapper) {
        this.dbndWrapper = dbndWrapper;
    }

    @Override
    protected void append(LoggingEvent event) {
        appendInternal(event);
    }

    public void appendInternal(LoggingEvent event) {
        dbndWrapper.logTask(event, layout.format(event));
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
}
