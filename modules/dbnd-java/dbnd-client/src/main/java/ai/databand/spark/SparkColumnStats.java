package ai.databand.spark;

import ai.databand.log.HistogramRequest;
import ai.databand.log.LogDatasetRequest;
import ai.databand.parameters.Histogram;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.histograms.NumericSummary;
import ai.databand.schema.histograms.Summary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;

public class SparkColumnStats {

    private final Dataset<?> dataset;
    private final LogDatasetRequest params;

    public SparkColumnStats(Dataset<?> dataset, LogDatasetRequest params) {
        this.dataset = dataset.alias(String.format("%s_%s", DBND_INTERNAL_ALIAS, "STATS"));
        this.params = params;
    }

    public List<ColumnStats> values() {
        if (!params.getWithStats()) {
            return Collections.emptyList();
        }
        Histogram histogram = new Histogram("columnStat", dataset, new HistogramRequest().onlyStats());
        Map<String, String> columnTypes = new HashMap<>();
        for (StructField field : dataset.schema().fields()) {
            columnTypes.put(field.name(), field.dataType().typeName());
        }
        // calculate summary
        histogram.summary();
        Map<String, Summary> summaries = histogram.getSummaries();
        List<ColumnStats> result = new ArrayList<>(summaries.size());
        for (Map.Entry<String, Summary> col : summaries.entrySet()) {
            Summary columnMetrics = col.getValue();
            ColumnStats columnStats = new ColumnStats()
                .setColumnName(col.getKey())
                .setColumnType(columnTypes.get(col.getKey()))
                .setRecordsCount(columnMetrics.getCount())
                .setDistinctCount(columnMetrics.getDistinct());
            if (columnMetrics instanceof NumericSummary) {
                NumericSummary numericSummary = (NumericSummary) columnMetrics;
                columnStats.setMeanValue(numericSummary.getMean())
                    .setMinValue(numericSummary.getMin())
                    .setMaxValue(numericSummary.getMax())
                    .setStdValue(numericSummary.getStd())
                    .setQuartile1(numericSummary.get_25())
                    .setQuartile2(numericSummary.get_50())
                    .setQuartile3(numericSummary.get_75());
            }
            result.add(columnStats);
        }
        return result;
    }

}
