package ai.databand.schema;

public enum DatasetOperationTypes {
    READ("read"), WRITE("write"), DELETE("delete");

    private final String name;

    DatasetOperationTypes(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
