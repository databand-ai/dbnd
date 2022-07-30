/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.log;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Allows to customize dataframe reporting. Only boolean, numeric and string columns are supported.
 */
public class HistogramRequest {

    private final Set<String> includeColumns = new HashSet<>(1);
    private final Set<String> excludeColumns = new HashSet<>(1);

    private boolean includeAllNumeric;
    private boolean includeAllString;
    private boolean includeAllBoolean;

    private boolean onlyStats;

    private boolean approxDistinct;

    private final boolean enabled;

    /**
     * Default constructor assumes full descriptive statistics and histograms calculation
     */
    public HistogramRequest() {
        this.enabled = true;
    }

    /**
     * @param all
     */
    public HistogramRequest(boolean all) {
        this.enabled = all;
        if (all) {
            this.includeAllString = true;
            this.includeAllBoolean = true;
            this.includeAllNumeric = true;
        }
    }

    /**
     * Column names to include into report. Unless provided, all columns will be reported, except of excluded.
     * If provided, only these columns will be included to the report.
     *
     * @param columns
     * @return
     */
    public HistogramRequest includeColumns(Collection<String> columns) {
        this.includeColumns.addAll(columns);
        return this;
    }

    /**
     * Exclude columns from calculations.
     *
     * @param columns
     * @return
     */
    public HistogramRequest excludeColumns(Collection<String> columns) {
        this.excludeColumns.addAll(columns);
        return this;
    }

    /**
     * Include all boolean columns.
     *
     * @return
     */
    public HistogramRequest includeAllBoolean() {
        this.includeAllBoolean = true;
        return this;
    }

    /**
     * Include all numeric columns.
     *
     * @return
     */
    public HistogramRequest includeAllNumeric() {
        this.includeAllNumeric = true;
        return this;
    }

    /**
     * Include all string columns.
     *
     * @return
     */
    public HistogramRequest includeAllString() {
        this.includeAllString = true;
        return this;
    }

    /**
     * Generate only descriptive statistics.
     *
     * @return
     */
    public HistogramRequest onlyStats() {
        this.onlyStats = true;
        return this;
    }

    /**
     * Use approximate distinct calculation method. May speed up calculations.
     *
     * @return
     */
    public HistogramRequest approxDistinct() {
        this.approxDistinct = true;
        return this;
    }

    protected Set<String> getIncludeColumns() {
        return includeColumns;
    }

    public Set<String> getExcludeColumns() {
        return excludeColumns;
    }

    public boolean isIncludeAllBoolean() {
        return includeAllBoolean;
    }

    public boolean isIncludeAllNumeric() {
        return includeAllNumeric;
    }

    public boolean isIncludeAllString() {
        return includeAllString;
    }

    public boolean isOnlyStats() {
        return onlyStats;
    }

    public boolean isApproxDistinct() {
        return approxDistinct;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isExcluded(String column) {
        return excludeColumns.contains(column);
    }

    /**
     * Generate report for all columns.
     *
     * @return
     */
    public static HistogramRequest ALL() {
        return new HistogramRequest(true);
    }

    /**
     * Generate report only for string columns.
     *
     * @return
     */
    public static HistogramRequest ALL_STRING() {
        return new HistogramRequest().includeAllString();
    }

    /**
     * Generate report only for boolean columns.
     *
     * @return
     */
    public static HistogramRequest ALL_BOOLEAN() {
        return new HistogramRequest().includeAllBoolean();
    }

    /**
     * Generate report only for numeric columns.
     *
     * @return
     */
    public static HistogramRequest ALL_NUMERIC() {
        return new HistogramRequest().includeAllNumeric();
    }

    /**
     * Do not generate report at all.
     *
     * @return
     */
    public static HistogramRequest NONE() {
        return new HistogramRequest(false);
    }

    /**
     * Default request assumes all columns.
     *
     * @return
     */
    public static HistogramRequest DEFAULT() {
        return ALL();
    }

}
