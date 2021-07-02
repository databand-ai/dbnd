package ai.databand.parameters;

import java.util.List;

public interface TaskParameterPreview<T> {

    String compact(T input);

    String full(T input);

    String typeName(Class<T> input);

    String schema(T input);

    List<Long> dimensions(T input);
}
