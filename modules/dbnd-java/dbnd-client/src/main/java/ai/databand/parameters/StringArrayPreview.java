/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.parameters;

import java.util.Collections;
import java.util.List;

public class StringArrayPreview implements TaskParameterPreview<String[]> {

    @Override
    public String compact(String[] input) {
        if (input == null) {
            return "[]";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        int idx = 0;
        for (String part : input) {
            if (builder.length() > 32) {
                builder.append("...");
                break;
            }
            builder.append(part);
            if (idx < input.length - 1) {
                builder.append(", ");
                idx++;
            }
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public String full(String[] input) {
        if (input == null) {
            return "[]";
        }
        return String.join(", ", input);
    }

    @Override
    public String typeName(Class<String[]> input) {
        return "String[]";
    }

    @Override
    public String schema(String[] input) {
        return "";
    }

    @Override
    public List<Long> dimensions(String[] input) {
        return Collections.emptyList();
    }
}
