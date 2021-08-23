package ai.databand.schema.histograms;

import java.util.HashMap;
import java.util.Map;

public class NumericSummary implements Summary {

    private final ColumnSummary columnSummary;

    private final double max;

    private final double mean;

    private final double min;

    private final double stddev;

    private final double _25;
    private final double _50;
    private final double _75;

    public NumericSummary(ColumnSummary columnSummary,
                          double max,
                          double mean,
                          double min,
                          double stddev,
                          double _25,
                          double _50,
                          double _75) {
        this.columnSummary = columnSummary;
        this.max = max;
        this.mean = mean;
        this.min = min;
        this.stddev = stddev;
        this._25 = _25;
        this._50 = _50;
        this._75 = _75;
    }

    public long getCount() {
        return columnSummary.getCount();
    }

    public long getDistinct() {
        return columnSummary.getDistinct();
    }

    public long getNonNull() {
        return columnSummary.getNonNull();
    }

    public long getNullCount() {
        return columnSummary.getNullCount();
    }

    public String getType() {
        return columnSummary.getType();
    }

    public double getMax() {
        return max;
    }

    public double getMean() {
        return mean;
    }

    public double getMin() {
        return min;
    }

    public double getStd() {
        return stddev;
    }

    public double getStddev() {
        return stddev;
    }

    public double get_25() {
        return _25;
    }

    public double get_50() {
        return _50;
    }

    public double get_75() {
        return _75;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>(12);
        result.put("max", max);
        result.put("mean", mean);
        result.put("min", min);
        result.put("stddev", stddev);
        result.put("std", stddev);
        result.put("25%", _25);
        result.put("50%", _50);
        result.put("75%", _75);
        result.putAll(columnSummary.toMap());
        return result;
    }

}
