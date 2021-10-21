package ai.databand.parameters;

import java.util.Collections;
import java.util.List;

public class StringPreview implements TaskParameterPreview<String> {

    @Override
    public String compact(String input) {
        return input == null ? "null" : input;
    }

    @Override
    public String full(String input) {
        return input == null ? "null" : input;
    }

    @Override
    public String typeName(Class<String> input) {
        return input.getTypeName();
    }

    @Override
    public String schema(String input) {
        return "";
    }

    @Override
    public List<Long> dimensions(String input) {
        return Collections.emptyList();
    }
}
