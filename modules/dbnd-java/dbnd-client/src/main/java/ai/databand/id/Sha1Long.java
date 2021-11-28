package ai.databand.id;

import java.util.Base64;

public class Sha1Long {

    private final String value;

    public Sha1Long(String namespace, String value) {
        this.value = Base64.getEncoder().encodeToString(new Sha1(namespace, value).value());
    }

    @Override
    public String toString() {
        return value;
    }
}
