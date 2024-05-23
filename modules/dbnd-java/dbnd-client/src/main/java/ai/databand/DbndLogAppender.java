/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class DbndLogAppender extends AppenderSkeleton {

    private final DbndWrapper dbndWrapper;

    public DbndLogAppender(DbndWrapper dbndWrapper) {
        this.dbndWrapper = dbndWrapper;
    }

    public void addAppenders(final String... loggers) {
        Logger rlog = Logger.getRootLogger();
        boolean logAdditivity = rlog.getAdditivity();
        if(!logAdditivity) {
            // Log additivity is "False" on Spark.
            // We need to enable it or otherwise Spark logs are not correctly collected locally.
            rlog.setAdditivity(true);
        }

        if(loggers.length > 0) {
            for(String logger : loggers) {
                Logger.getLogger(logger).addAppender(this);
            }
        } else {
            rlog.addAppender(this);
        }

        // restore original value, no side effects
        rlog.setAdditivity(logAdditivity);
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
