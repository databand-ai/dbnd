package ai.databand.id;

import java.util.Objects;
import java.util.UUID;

public class Uuid5 {

    public static UUID NAMESPACE_DBND = Uuid5Raw.fromString(Uuid5Raw.NAMESPACE_DNS, "databand.ai");
    public static UUID NAMESPACE_DBND_JOB = Uuid5Raw.fromString(NAMESPACE_DBND, "job");
    public static UUID NAMESPACE_DBND_RUN = Uuid5Raw.fromString(NAMESPACE_DBND, "run");
    public static UUID NAMESPACE_DBND_TASK_DEF = Uuid5Raw.fromString(NAMESPACE_DBND, "task_definition");
    private final String value;

    public Uuid5(UUID namespace, String name) {
        this.value = Uuid5Raw.fromString(namespace, name).toString();
    }

    public Uuid5(String namespace, String name) {
        Sha1 digest = new Sha1(namespace, name);

        UUID uuid = Uuid5Raw.bytesToUuid(digest.value());
        this.value = uuid.toString();
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Uuid5 uuid5 = (Uuid5) o;
        return value.equals(uuid5.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
