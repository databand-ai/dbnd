package ai.databand.schema.histograms;

import java.util.Map;

public interface Summary {

    long getCount();

    long getDistinct();

    long getNonNull();

    long getNullCount();

    String getType();

    Map<String, Object> toMap();

}
