/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.parameters;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

class StringArrayPreviewTest {

    @Test
    public void testPreview() {
        StringArrayPreview preview = new StringArrayPreview();

        String[] input = new String[]{"65daysofstatic", "Radio Protector", "Between"};

        assertThat("Wrong compact preview", preview.compact(input), Matchers.equalTo("[65daysofstatic, Radio Protector, ...]"));
        assertThat("Wrong full preview", preview.full(input), Matchers.equalTo("65daysofstatic, Radio Protector, Between"));
    }
}
