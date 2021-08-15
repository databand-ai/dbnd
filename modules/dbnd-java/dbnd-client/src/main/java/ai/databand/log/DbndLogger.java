package ai.databand.log;

import ai.databand.DbndWrapper;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
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

    /**
     * Report success dataset operation. Schema and preview will automatically be calculated.
     *
     * @param path data path (S3, filesystem, GCS etc)
     * @param type operation type — read/write
     * @param data spark dataset
     */
    public static void logDatasetOperation(String path,
                                           DatasetOperationType type,
                                           Dataset<?> data) {
        try {
            DbndWrapper.instance().logDatasetOperation(path, type, DatasetOperationStatus.OK, data, true, true);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log dataset operation");
            e.printStackTrace();
        }
    }

    /**
     * Report dataset operation. Schema and preview will automatically be calculated.
     *
     * @param path   data path (S3, filesystem, GCS etc)
     * @param type   operation type — read/write
     * @param status operation status — success/failure
     * @param data   spark dataset
     */
    public static void logDatasetOperation(String path,
                                           DatasetOperationType type,
                                           DatasetOperationStatus status,
                                           Dataset<?> data) {
        try {
            DbndWrapper.instance().logDatasetOperation(path, type, status, data, true, true);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log dataset operation");
            e.printStackTrace();
        }
    }

    /**
     * Report dataset operation. Schema will be automatically calculated.
     *
     * @param path        data path (S3, filesystem, GCS etc)
     * @param type        operation type — read/write
     * @param status      operation status — success/failure
     * @param data        spark dataset
     * @param withPreview pass true if you want to generate preview (may impact performance)
     */
    public static void logDatasetOperation(String path,
                                           DatasetOperationType type,
                                           DatasetOperationStatus status,
                                           Dataset<?> data,
                                           boolean withPreview) {
        try {
            DbndWrapper.instance().logDatasetOperation(path, type, status, data, withPreview, true);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log dataset operation");
            e.printStackTrace();
        }
    }

    /**
     * Report dataset operation.
     *
     * @param path        data path (S3, filesystem, GCS etc)
     * @param type        operation type — read/write
     * @param status      operation status — success/failure
     * @param data        spark dataset
     * @param withPreview pass true if you want to generate preview (may impact performance)
     * @param withSchema  pass true if you want to calculate dataset schema
     */
    public static void logDatasetOperation(String path,
                                           DatasetOperationType type,
                                           DatasetOperationStatus status,
                                           Dataset<?> data,
                                           boolean withPreview,
                                           boolean withSchema) {
        try {
            DbndWrapper.instance().logDatasetOperation(path, type, status, data, withPreview, withSchema);
        } catch (Throwable e) {
            System.out.println("DbndLogger: Unable to log dataset operation");
            e.printStackTrace();
        }
    }

}
