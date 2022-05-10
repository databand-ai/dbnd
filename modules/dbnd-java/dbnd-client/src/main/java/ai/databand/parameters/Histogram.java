package ai.databand.parameters;

import ai.databand.log.HistogramRequest;
import ai.databand.schema.histograms.ColumnSummary;
import ai.databand.schema.histograms.NumericSummary;
import ai.databand.schema.histograms.Summary;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.FractionalType;
import org.apache.spark.sql.types.IntegralType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ai.databand.DbndPropertyNames.DBND_INTERNAL_ALIAS;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class Histogram {

    private static final int MAX_NUMERIC_BUCKETS_COUNT = 20;
    private static final int MAX_CATEGORICAL_BUCKETS_COUNT = 50;

    private final String dfKey;
    private final Dataset<?> dataset;
    private final HistogramRequest req;
    private final Map<String, Object> result;
    private final Map<String, Summary> summaries;

    public Histogram(String key, Dataset<?> dataset, HistogramRequest histogramRequest) {
        this.dfKey = key;
        this.dataset = dataset.alias(String.format("%s_%s", DBND_INTERNAL_ALIAS, "HISTOGRAM"));
        this.req = histogramRequest;
        result = new HashMap<>(1);
        summaries = new HashMap<>(1);
    }

    protected <T> Seq<T> seq(List<T> list) {
        return JavaConverters.collectionAsScalaIterableConverter(list).asScala().toSeq();
    }

    public Map<String, Object> metricValues() {
        result.put(String.format("%s.stats", dfKey), summary());

        if (req.isEnabled() && !req.isOnlyStats()) {
            Map<String, Object> histograms = new HashMap<>(1);
            if (req.isIncludeAllNumeric()) {
                histograms.putAll(numericHistograms());
            }
            if (req.isIncludeAllString()) {
                histograms.putAll(categoricalHistograms(StringType.class));
            }
            if (req.isIncludeAllBoolean()) {
                histograms.putAll(categoricalHistograms(BooleanType.class));
            }
            result.put(String.format("%s.histograms", dfKey), histograms);
        }

        return result;
    }

    public Map<String, Map<String, Object>> summary() {
        Dataset<Row> summaryDf = dataset.summary();

        Map<String, Integer> colToIdx = new HashMap<>();
        for (int i = 0; i < summaryDf.columns().length; i++) {
            colToIdx.put(summaryDf.columns()[i], i);
        }

        List<Row> rawSummary = summaryDf.collectAsList();
        Map<String, Row> summary = new HashMap<>(1);
        for (Row row : rawSummary) {
            summary.put(row.get(0).toString(), row);
        }

        // distinct, count, and non-null counts are calculated separately, because they are not included into default spark summary
        List<String> exprs = new ArrayList<>(1);
        for (StructField c : dataset.schema().fields()) {
            if (!isSimpleType(c.dataType()) || req.isExcluded(c.name())) {
                continue;
            }
            Column col = col(c.name());
            // for some reason spark didn't escape numeric column names like `10` in DISTINCT query
            // so we have to escape colum name with backticks manually
            exprs.add(String.format("count(DISTINCT `%s`) AS `%s_%s`", c.name(), c.name(), "distinct"));
            exprs.add(count(col).alias(String.format("%s_%s", c.name(), "non-null")).toString());
            exprs.add(count(when(col.isNull(), 1)).alias(String.format("%s_%s", c.name(), "count_null")).toString());
        }

        Dataset<Row> countsDf = dataset.selectExpr(seq(exprs));
        Row rawCounts = countsDf.collectAsList().get(0);
        String[] countsColumns = countsDf.columns();

        Map<String, Object> counts = new HashMap<>(1);

        for (int i = 0; i < countsColumns.length; i++) {
            counts.put(countsColumns[i], rawCounts.get(i));
        }

        Map<String, Map<String, Object>> stats = new HashMap<>(1);

        for (StructField c : dataset.schema().fields()) {
            if (!isSimpleType(c.dataType()) || req.isExcluded(c.name())) {
                continue;
            }
            Summary columnSummary = null;
            long nonNull = Long.parseLong(counts.get(String.format("%s_%s", c.name(), "non-null")).toString());
            long countNull = Long.parseLong(counts.get(String.format("%s_%s", c.name(), "count_null")).toString());
            if (c.dataType() instanceof NumericType) {
                int idx = colToIdx.get(c.name());
                columnSummary = new NumericSummary(
                    new ColumnSummary(
                        nonNull + countNull,
                        Long.parseLong(counts.get(String.format("%s_%s", c.name(), "distinct")).toString()),
                        nonNull,
                        countNull,
                        (c.dataType() instanceof FractionalType) ? "double" : "integer"
                    ),
                    Double.parseDouble(summary.get("max").get(idx).toString()),
                    Double.parseDouble(summary.get("mean").get(idx).toString()),
                    Double.parseDouble(summary.get("min").get(idx).toString()),
                    Double.parseDouble(summary.get("stddev").get(idx).toString()),
                    Double.parseDouble(summary.get("25%").get(idx).toString()),
                    Double.parseDouble(summary.get("50%").get(idx).toString()),
                    Double.parseDouble(summary.get("75%").get(idx).toString())
                );
            } else if (c.dataType() instanceof StringType || c.dataType() instanceof BooleanType) {
                columnSummary = new ColumnSummary(
                    nonNull + countNull,
                    Long.parseLong(counts.get(String.format("%s_%s", c.name(), "distinct")).toString()),
                    nonNull,
                    countNull,
                    (c.dataType() instanceof StringType) ? "string" : "boolean"
                );
            }

            Map<String, Object> columnSummaryMap = columnSummary.toMap();
            stats.put(c.name(), columnSummaryMap);
            for (Map.Entry<String, Object> entry : columnSummaryMap.entrySet()) {
                result.put(String.format("%s.%s.%s", dfKey, c.name(), entry.getKey()), entry.getValue());
            }
            summaries.put(c.name(), columnSummary);
        }
        return stats;
    }

    public Map<String, Summary> getSummaries() {
        return summaries;
    }

    protected boolean isSimpleType(DataType dt) {
        return dt instanceof NumericType || dt instanceof StringType || dt instanceof BooleanType;
    }

    protected Map<String, Object[][]> numericHistograms() {
        List<Column> numericColumns = new ArrayList<>(1);
        List<String> histogramsExpr = new ArrayList<>(1);
        Map<String, Object[]> namedBuckets = new HashMap<>(1);
        for (StructField c : dataset.schema().fields()) {
            if (!(c.dataType() instanceof NumericType) || req.isExcluded(c.name())) {
                continue;
            }
            numericColumns.add(col(c.name()));

            long distinct = (long) result.get(String.format("%s.%s.%s", dfKey, c.name(), "distinct"));

            double minv = (double) result.get(String.format("%s.%s.%s", dfKey, c.name(), "min"));
            double maxv = (double) result.get(String.format("%s.%s.%s", dfKey, c.name(), "max"));

            int bucketsCount = (int) Math.min(distinct, MAX_NUMERIC_BUCKETS_COUNT);

            double inc;

            if (c.dataType() instanceof IntegralType) {
                inc = (int) ((maxv - minv) / bucketsCount);
            } else {
                inc = (maxv - minv) * 1.0 / bucketsCount;
            }

            Object[] buckets = new Object[bucketsCount + 1];
            for (int i = 0; i < bucketsCount; i++) {
                buckets[i] = i * inc + minv;
            }
            buckets[bucketsCount] = maxv;

            namedBuckets.put(c.name(), buckets);

            for (int i = 0; i < buckets.length - 1; i++) {
                histogramsExpr.add(
                    count(
                        when(
                            col(c.name()).geq(buckets[i])
                                .and(i == buckets.length - 2 ? col(c.name()).leq(buckets[i + 1]) : col(c.name()).lt(buckets[i + 1])), 1
                        )
                    ).alias(String.format("%s_%s", c.name(), i)).toString()
                );
            }
        }

        Dataset<Row> histogramsDf = dataset.select(seq(numericColumns)).selectExpr(seq(histogramsExpr));
        Row histograms = histogramsDf.collectAsList().get(0);

        Map<String, Object[][]> histogramsResult = new HashMap<>(1);

        for (String column : namedBuckets.keySet()) {
            Object[] buckets = namedBuckets.get(column);
            Object[] bucketCounts = new Object[buckets.length];

            for (int i = 0; i < buckets.length - 1; i++) {
                bucketCounts[i] = histograms.getAs(String.format("%s_%s", column, i));
            }
            histogramsResult.put(column, new Object[][]{bucketCounts, buckets});
        }
        return histogramsResult;
    }

    List<Dataset<Row>> columnsOfType(Class<?> dataType) {
        return Arrays.stream(dataset.schema().fields())
            .filter(f -> dataType.isInstance(f.dataType()))
            .filter(f -> !req.isExcluded(f.name()))
            .map(f -> dataset.select(f.name()))
            .collect(Collectors.toList());
    }

    protected Map<String, List<List<Object>>> categoricalHistograms(Class<?> dataType) {
        List<Dataset<Row>> columnsDf = columnsOfType(dataType);

        if (columnsDf.isEmpty()) {
            return Collections.emptyMap();
        }

        Dataset<Row> valueCounts = null;

        for (Dataset<Row> column : columnsDf) {
            String columnName = column.schema().names()[0];

            Dataset<Row> columnCounts = column.groupBy(columnName)
                .count()
                .orderBy(desc("count"))
                .withColumn("column_name", lit(columnName))
                .limit(MAX_CATEGORICAL_BUCKETS_COUNT - 1);

            if (valueCounts == null) {
                valueCounts = columnCounts;
            } else {
                valueCounts = valueCounts.union(columnCounts);
            }
        }

        Map<String, List<List<Object>>> histogramsResult = new HashMap<>(1);

        for (Row row : valueCounts.collectAsList()) {
            if (row.get(0) == null) {
                continue;
            }
            String bucket = row.get(0).toString();
            long count = row.getLong(1);
            String columnName = row.getString(2);

            List<List<Object>> columnHistogram = histogramsResult.computeIfAbsent(columnName, c -> {
                List<List<Object>> pair = new ArrayList<>(2);
                pair.add(new ArrayList<>(1));
                pair.add(new ArrayList<>(1));
                return pair;
            });

            columnHistogram.get(0).add(count);
            columnHistogram.get(1).add(bucket);
        }

        // add "others"
        for (Map.Entry<String, List<List<Object>>> column : histogramsResult.entrySet()) {
            Summary summary = summaries.get(column.getKey());
            long distinct = summary.getDistinct();
            if (distinct < MAX_CATEGORICAL_BUCKETS_COUNT) {
                continue;
            }

            long total = summary.getCount();
            long histogramSumCount = column.getValue().get(0).stream().mapToLong(f -> (Long) f).sum();
            long othersCount = total - histogramSumCount;

            column.getValue().get(0).add(othersCount);
            column.getValue().get(1).add("_others");
        }

        return histogramsResult;
    }

}
