package ai.databand.parameters;

import java.util.Collections;
import java.util.List;

public class ObjectPreview implements TaskParameterPreview<Object> {

    @Override
    public String compact(Object input) {
        return String.valueOf(input);
    }

    @Override
    public String full(Object input) {
        return String.valueOf(input);
    }

    @Override
    public String typeName(Class<Object> input) {
        return input.getTypeName();
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
