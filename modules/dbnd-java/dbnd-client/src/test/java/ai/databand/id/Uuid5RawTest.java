/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.id;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class Uuid5RawTest {

    @Test
    public void testUuid5() {
        UUID namespace = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        String name = "test";

        String excepted = "4be0643f-1d98-573b-97cd-ca98a65347dd";
        String actual = Uuid5Raw.fromString(namespace, name).toString();

        MatcherAssert.assertThat("UUID5 should be generated properly", actual, Matchers.equalTo(excepted));
    }

    @Test
    public void testFromBytes() {
        UUID excepted = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        byte[] bytes = Uuid5Raw.uuidToBytes(excepted);
        UUID actual = Uuid5Raw.bytesToUuid(bytes);

        MatcherAssert.assertThat("UUID5 should be properly created from bytes", actual, Matchers.equalTo(excepted));
    }

    @Test
    public void testToBytes() {
        UUID uuid = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        byte[] excepted = new byte[]{107, -89, -72, 16, -99, -83, 17, -47, -128, -76, 0, -64, 79, -44, 48, -56};
        byte[] actual = Uuid5Raw.uuidToBytes(uuid);

        MatcherAssert.assertThat("Wrong uuid to byte[] conversion", actual, Matchers.equalTo(excepted));
    }
}
