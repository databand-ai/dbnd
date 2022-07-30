/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public enum DatasetOperationType {
    READ("read"), WRITE("write"), DELETE("delete");

    private final String name;

    DatasetOperationType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
