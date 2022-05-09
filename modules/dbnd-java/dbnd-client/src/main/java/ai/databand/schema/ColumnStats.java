package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnStats {
    String columnName;
    String columnType;

    Long recordsCount;

    Long distinctCount;

    // Metric for non-numeric column type
    Long uniqueCount;

    // Most frequent value
    Object mostFreqValue;
    Long mostFreqValueCount;

    // numeric column type metrics
    Double meanValue;
    Double minValue;
    Double maxValue;
    Double stdValue;

    // percentiles
    @JsonProperty("quartile_1")
    Double quartile1;
    @JsonProperty("quartile_2")
    Double quartile2;
    @JsonProperty("quartile_3")
    Double quartile3;

    public String getColumnName() {
        return columnName;
    }

    public ColumnStats setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public String getColumnType() {
        return columnType;
    }

    public ColumnStats setColumnType(String columnType) {
        this.columnType = columnType;
        return this;
    }

    public Long getRecordsCount() {
        return recordsCount;
    }

    public ColumnStats setRecordsCount(Long recordsCount) {
        this.recordsCount = recordsCount;
        return this;
    }

    public Long getDistinctCount() {
        return distinctCount;
    }

    public ColumnStats setDistinctCount(Long distinctCount) {
        this.distinctCount = distinctCount;
        return this;
    }

    public Long getUniqueCount() {
        return uniqueCount;
    }

    public ColumnStats setUniqueCount(Long uniqueCount) {
        this.uniqueCount = uniqueCount;
        return this;
    }

    public Object getMostFreqValue() {
        return mostFreqValue;
    }

    public ColumnStats setMostFreqValue(Object mostFreqValue) {
        this.mostFreqValue = mostFreqValue;
        return this;
    }

    public Long getMostFreqValueCount() {
        return mostFreqValueCount;
    }

    public ColumnStats setMostFreqValueCount(Long mostFreqValueCount) {
        this.mostFreqValueCount = mostFreqValueCount;
        return this;
    }

    public Double getMeanValue() {
        return meanValue;
    }

    public ColumnStats setMeanValue(Double meanValue) {
        this.meanValue = meanValue;
        return this;
    }

    public Double getMinValue() {
        return minValue;
    }

    public ColumnStats setMinValue(Double minValue) {
        this.minValue = minValue;
        return this;
    }

    public Double getMaxValue() {
        return maxValue;
    }

    public ColumnStats setMaxValue(Double maxValue) {
        this.maxValue = maxValue;
        return this;
    }

    public Double getStdValue() {
        return stdValue;
    }

    public ColumnStats setStdValue(Double stdValue) {
        this.stdValue = stdValue;
        return this;
    }

    public Double getQuartile1() {
        return quartile1;
    }

    public ColumnStats setQuartile1(Double quartile1) {
        this.quartile1 = quartile1;
        return this;
    }

    public Double getQuartile2() {
        return quartile2;
    }

    public ColumnStats setQuartile2(Double quartile2) {
        this.quartile2 = quartile2;
        return this;
    }

    public Double getQuartile3() {
        return quartile3;
    }

    public ColumnStats setQuartile3(Double quartile3) {
        this.quartile3 = quartile3;
        return this;
    }
}
