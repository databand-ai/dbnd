package ai.databand.id;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Sha1 {

    private final byte[] value;

    public Sha1(String namespace, String name) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] namespaceBytes = namespace.getBytes();
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            byte[] both = Arrays.copyOf(namespaceBytes, namespaceBytes.length + nameBytes.length);
            System.arraycopy(nameBytes, 0, both, namespaceBytes.length, nameBytes.length);
            value = md.digest(both);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unable to get SHA-1 digest");
        }
    }

    public byte[] value() {
        return value;
    }
}
