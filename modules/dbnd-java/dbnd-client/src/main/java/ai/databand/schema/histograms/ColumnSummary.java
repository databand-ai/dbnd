/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema.histograms;

import java.util.HashMap;
import java.util.Map;

public class ColumnSummary implements Summary {

    private final long count;
    private final long distinct;
    private final long nonNull;
    private final long nullCount;
    private final String type;

    public ColumnSummary(long count, long distinct, long nonNull, long nullCount, String type) {
        this.count = count;
        this.distinct = distinct;
        this.nonNull = nonNull;
        this.nullCount = nullCount;
        this.type = type;
    }

    public long getCount() {
        return count;
    }

    public long getDistinct() {
        return distinct;
    }

    public long getNonNull() {
        return nonNull;
    }

    public long getNullCount() {
        return nullCount;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>(5);
        result.put("count", count);
        result.put("distinct", distinct);
        result.put("non-null", nonNull);
        result.put("null-count", nullCount);
        result.put("type", type);
        return result;
    }
}
