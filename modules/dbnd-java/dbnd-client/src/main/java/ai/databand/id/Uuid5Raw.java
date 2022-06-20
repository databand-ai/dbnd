package ai.databand.id;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;

/**
 * UUID5 generator.
 */
public class Uuid5Raw {

    // Predefined DNS namespace
    public static final UUID NAMESPACE_DNS = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    /**
     * UUID5 is UUID(SHA-1(namespace + name)).
     * See <a href="https://www.rfc-editor.org/rfc/rfc4122">RFC4122</a> for details.
     *
     * @param namespace
     * @param name
     * @return
     */
    public static UUID fromString(UUID namespace, String name) {
        try {
            // step 0: check input
            Objects.requireNonNull(namespace, "UUID5 namespace should not be null");
            Objects.requireNonNull(name, "UUID5 name should not be null");
            // step 1: generate hash
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(uuidToBytes(namespace));
            md.update(name.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest();
            // step 2: set most significant bits of octet 6 to version "5"
            digest[6] &= 0x0F;
            digest[6] |= 0x50;
            // step 3: set most significant bits of octet 8 to variant "IETF"
            digest[8] &= 0x3F;
            digest[8] |= 0x80;
            // step 4: take first 16 bytes from the digest and make UUID
            return bytesToUuid(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new InternalError("SHA-1 not supported");
        }
    }

    /**
     * Convert byte array to UUID using unrolled loop.
     *
     * @param bytes
     * @return
     */
    protected static UUID bytesToUuid(byte[] bytes) {
        long msb = ((long) bytes[0] << 56)
            | ((long) bytes[1] & 0xff) << 48
            | ((long) bytes[2] & 0xff) << 40
            | ((long) bytes[3] & 0xff) << 32
            | ((long) bytes[4] & 0xff) << 24
            | ((long) bytes[5] & 0xff) << 16
            | ((long) bytes[6] & 0xff) << 8
            | ((long) bytes[7] & 0xff);

        long lsb = ((long) bytes[8] << 56)
            | ((long) bytes[9] & 0xff) << 48
            | ((long) bytes[10] & 0xff) << 40
            | ((long) bytes[11] & 0xff) << 32
            | ((long) bytes[12] & 0xff) << 24
            | ((long) bytes[13] & 0xff) << 16
            | ((long) bytes[14] & 0xff) << 8
            | ((long) bytes[15] & 0xff);

        return new UUID(msb, lsb);
    }

    /**
     * Convert UUID to byte array.
     *
     * UUID is 128 bits.
     * Array is composed via 64 most significant bits and 64 least significant bits in a big-endian order.
     * This is unrolled loop.
     *
     * @param uuid
     * @return
     */
    protected static byte[] uuidToBytes(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        return new byte[] {
            (byte) (msb >> 56),
            (byte) (msb >> 48),
            (byte) (msb >> 40),
            (byte) (msb >> 32),
            (byte) (msb >> 24),
            (byte) (msb >> 16),
            (byte) (msb >> 8),
            (byte) msb,
            (byte) (lsb >> 56),
            (byte) (lsb >> 48),
            (byte) (lsb >> 40),
            (byte) (lsb >> 32),
            (byte) (lsb >> 24),
            (byte) (lsb >> 16),
            (byte) (lsb >> 8),
            (byte) lsb};
    }

}
