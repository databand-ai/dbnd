/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

public enum DatasetOperationStatus {
    OK("OK"), NOK("NOK");

    private final String name;

    DatasetOperationStatus(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
