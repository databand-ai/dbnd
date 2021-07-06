package ai.databand.schema;

public enum DatasetOperationStatuses {
    OK("OK"), NOK("NOK");

    private final String name;

    DatasetOperationStatuses(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
