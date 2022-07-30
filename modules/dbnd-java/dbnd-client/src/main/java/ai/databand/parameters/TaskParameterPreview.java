/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.parameters;

import java.util.List;

public interface TaskParameterPreview<T> {

    String compact(T input);

    String full(T input);

    String typeName(Class<T> input);

    Object schema(T input);

    List<Long> dimensions(T input);
}
