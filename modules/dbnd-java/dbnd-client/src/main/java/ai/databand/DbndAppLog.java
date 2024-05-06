/*
 * © Copyright Databand.ai, an IBM Company 2024
 */

package ai.databand;

import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DbndAppLog {

    private final org.slf4j.Logger LOG ;

    public DbndAppLog(final org.slf4j.Logger log4j) {
        this.LOG = log4j;
    }

    public final static String LOG_PREFIX= "<<DBND>> "; // make it clearly visible in Spark logs to not confuse it with regular Spark execution logs
    public static void printf(final org.slf4j.event.Level lvl, final String msg, final Object... args) {
        final LocalDateTime dt = LocalDateTime.now();
        final String timeStamp = dt.format(DateTimeFormatter.ofPattern("YY/MM/dd HH:mm:ss"));

        final StackTraceElement[] st = Thread.currentThread().getStackTrace();
        final StackTraceElement ste = st[2];

        final PrintStream outOrErr = lvl == org.slf4j.event.Level.ERROR ? System.err : System.out;

        String logInfos = String.format("%s %s %s.%s: %sp ", timeStamp, lvl, ste.getClassName(), ste.getMethodName(), LOG_PREFIX);
        outOrErr.printf(logInfos + msg, args);
    }

    public static void printfln(final org.slf4j.event.Level lvl, final String msg, final Object... args) {
        printf(lvl, msg + "%n", args);
    }

    public static void printfv(final String msg, final Object... args) {
        if(DbndWrapper.instance().config().isVerbose()) {
            printf(org.slf4j.event.Level.INFO, msg, args);
        }
    }

    public static void printfvln(final String msg, final Object... args) {
        printfv(msg + "%n", args);
    }

    public void info(final String msg, final Object... args) {
        LOG.info(DbndAppLog.LOG_PREFIX + msg + "%n", args);
    }

    public void warn(final String msg, final Object... args) {
        LOG.warn(DbndAppLog.LOG_PREFIX + msg + "%n", args);
    }

    public void error(final String msg, final Object... args) {
        LOG.error(DbndAppLog.LOG_PREFIX + msg + "%n", args);
    }

    public void verbose(final String msg, final Object... args) {
        if(DbndWrapper.instance().config().isVerbose()) {
            LOG.info(DbndAppLog.LOG_PREFIX + msg, args);
        }
    }
}
