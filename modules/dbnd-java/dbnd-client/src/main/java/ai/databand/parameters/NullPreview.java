/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.parameters;

import java.util.Collections;
import java.util.List;

public class NullPreview implements TaskParameterPreview<Object> {

    @Override
    public String compact(Object input) {
        return "";
    }

    @Override
    public String full(Object input) {
        return "";
    }

    @Override
    public String typeName(Class<Object> input) {
        return "";
    }

    @Override
    public String schema(Object input) {
        return "";
    }

    @Override
    public List<Long> dimensions(Object input) {
        return Collections.emptyList();
    }
}
