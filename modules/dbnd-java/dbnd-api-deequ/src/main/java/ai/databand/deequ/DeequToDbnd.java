/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.deequ;

import com.amazon.deequ.analyzers.runners.AnalyzerContext;
import com.amazon.deequ.metrics.Distribution;
import com.amazon.deequ.metrics.DistributionValue;
import com.amazon.deequ.metrics.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DeequToDbnd {

    private static final Logger LOG = LoggerFactory.getLogger(DeequToDbnd.class);

    private final String dfName;
    private final List<Metric<?>> deequMetrics;

    private static final Map<String, String> DEEQU_TO_SPARK = new HashMap<>();

    static {
        DEEQU_TO_SPARK.put("ApproxCountDistinct", "distinct");
        DEEQU_TO_SPARK.put("Minimum", "min");
        DEEQU_TO_SPARK.put("Maximum", "max");
        DEEQU_TO_SPARK.put("Mean", "mean");
        DEEQU_TO_SPARK.put("StandardDeviation", "stddev");
        DEEQU_TO_SPARK.put("Histogram", "histogram");
    }

    private static final Map<String, String> DEEQU_TO_DBND = new HashMap<>();

    static {
        DEEQU_TO_DBND.put("Boolean", "boolean");
        DEEQU_TO_DBND.put("Fractional", "double");
        DEEQU_TO_DBND.put("Integral", "integer");
        DEEQU_TO_DBND.put("String", "string");
        DEEQU_TO_DBND.put("Unknown", "string");
    }

    public DeequToDbnd(String dfName, AnalyzerContext analyzerContext) {
        this.dfName = dfName;
        deequMetrics = JavaConverters.seqAsJavaListConverter(analyzerContext.metricMap().values().toSeq()).asJava();
    }

    public Map<String, Object> metrics() {
        Map<String, Object> metrics = new HashMap<>(1);
        for (Metric<?> m : deequMetrics) {
            // skip histogram metrics
            if (m.value().isSuccess() && m.value().get() instanceof Distribution) {
                continue;
            }
            String metricKey = String.format("deequ.%s.%s.%s", dfName, m.instance(), m.name());
            if (m.value().isFailure()) {
                LOG.error("Deequ calculation failed for key [{}]. Reason: {}", metricKey, m.value().failed().get().getMessage());
            }
            Object value = m.value().isSuccess() ? m.value().get() : "Failure";
            metrics.put(metricKey, value);
        }
        return metrics;
    }

    public Map<String, Object> histograms() {
        return buildHistograms(dfName, deequMetrics);
    }

    public Map<String, Object> buildHistograms(String dfName, List<Metric<?>> deequMetrics) {
        Set<String> histogrammedCols = deequMetrics.stream()
            .filter(m -> "Histogram".equalsIgnoreCase(m.name()))
            .map(Metric::instance)
            .collect(Collectors.toSet());
        if (histogrammedCols.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> result = new HashMap<>(1);
        Map<String, Object> histograms = new HashMap<>(1);
        Map<String, Map<String, Object>> stats = histogrammedCols.stream().collect(Collectors.toMap((m) -> m, (m) -> new HashMap<>(1)));

        for (Metric<?> m : deequMetrics) {
            String col = m.instance();
            if (!histogrammedCols.contains(col)) {
                continue;
            }
            String sparkMetric = DEEQU_TO_SPARK.get(m.name());
            if (sparkMetric == null) {
                continue;
            }
            if ("histogram".equalsIgnoreCase(sparkMetric)) {
                // check we have real histogram
                Distribution distribution = (Distribution) m.value().get();

                Map<String, DistributionValue> binsAndValues = JavaConverters.mapAsJavaMapConverter(distribution.values()).asJava();
                if (isDistributionHistogram(binsAndValues)) {
                    // this is values distribution. Let's calculate column type based on this information
                    String type = guessColumnTypeByDistribution(binsAndValues);
                    stats.get(col).put("type", type);
                    continue;
                }

                Object[][] histogram = distributionToHistogram(binsAndValues);
                histograms.put(col, histogram);

                // let's guess column type
                // TODO: optimize
                if (!stats.get(col).containsKey("type")) {
                    Object[] bins = histogram[1];
                    if (allBooleans(bins)) {
                        stats.get(col).put("type", "boolean");
                    } else if (allIntegers(bins)) {
                        stats.get(col).put("type", "integer");
                    } else if (allDoubles(bins)) {
                        stats.get(col).put("type", "double");
                    } else {
                        stats.get(col).put("type", "string");
                    }
                }
                continue;
            }
            stats.get(col).put(sparkMetric, m.value().get());
            result.put(String.format("%s.%s.%s", dfName, col, sparkMetric), m.value().get());
        }

        result.put(String.format("%s.stats", dfName), stats);
        if (!histograms.isEmpty()) {
            result.put(String.format("%s.histograms", dfName), histograms);
        }
        return result;
    }

    public Object[][] distributionToHistogram(Map<String, DistributionValue> binsAndValues) {
        Object[] bins = new Object[binsAndValues.size()];
        Object[] values = new Object[binsAndValues.size()];
        int i = 0;
        for (Map.Entry<String, DistributionValue> entry : binsAndValues.entrySet()) {
            bins[i] = entry.getKey();
            values[i] = entry.getValue().absolute();
            i++;
        }
        return new Object[][]{values, bins};
    }

    protected boolean isDistributionHistogram(Map<String, DistributionValue> binsAndValues) {
        for (String type : DEEQU_TO_DBND.keySet()) {
            if (!binsAndValues.containsKey(type)) {
                return false;
            }
        }
        return true;
    }

    protected String guessColumnTypeByDistribution(Map<String, DistributionValue> binsAndValues) {
        for (Map.Entry<String, String> entry : DEEQU_TO_DBND.entrySet()) {
            if (binsAndValues.get(entry.getKey()).absolute() > 1) {
                return entry.getValue();
            }
        }
        return "string";
    }

    protected boolean allBooleans(Object[] values) {
        for (Object o : values) {
            if (o != null && !"true".equalsIgnoreCase(o.toString()) && !"false".equalsIgnoreCase(o.toString())) {
                return false;
            }
        }
        return true;
    }

    protected boolean allIntegers(Object[] values) {
        for (Object o : values) {
            if (o != null && o.toString().contains(".")) {
                return false;
            }
            if (o != null) {
                try {
                    Integer.parseInt(o.toString());
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }
        return true;
    }

    protected boolean allDoubles(Object[] values) {
        for (Object o : values) {
            if (o != null) {
                try {
                    Double.parseDouble(o.toString());
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }
        return true;
    }

}
