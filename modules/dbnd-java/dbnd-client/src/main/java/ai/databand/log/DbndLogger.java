package ai.databand.log;

import ai.databand.DbndWrapper;
import org.apache.spark.sql.Dataset;

/**
 * Report metrics and dataframes to Databand.
 * This class is safe to use in any circumstances and won't break client code.
 */
public class DbndLogger {

    /**
     * Report single metric.
     *
     * @param key   metric key
     * @param value metric value. Value will be converted using following rules:
     *              integers and doubles will be converted to integers and doubles
     *              strings will be converted to strings
     *              spark datasets will generate preview
     *              toString() will be called for the rest of the types
     */
    public static void logMetric(String key, Object value) {
        try {
            DbndWrapper.instance().logMetric(key, value);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log metric");
            e.printStackTrace();
        }
    }

    /**
     * Report Spark dataframe. Descriptive statistics and histograms will be generated for each column.
     *
     * @param key
     * @param value
     * @param withHistograms
     */
    public static void logDataframe(String key, Object value, boolean withHistograms) {
        try {
            DbndWrapper.instance().logDataframe(key, (Dataset<?>) value, withHistograms);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log dataframe");
            e.printStackTrace();
        }
    }

    /**
     * Report Spark dataframe. Descriptive statistics and histograms will be generated for each column.
     * Reporting can be customized using HistogramRequest object.
     *
     * @param key
     * @param value
     * @param histogramRequest
     */
    public static void logDataframe(String key, Object value, HistogramRequest histogramRequest) {
        try {
            DbndWrapper.instance().logDataframe(key, (Dataset<?>) value, histogramRequest);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log dataframe");
            e.printStackTrace();
        }
    }

}
