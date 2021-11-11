package ai.databand.parameters;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class StringPreviewTest {

    @Test
    public void testPreview() {
        StringPreview preview = new StringPreview();
        assertThat("Wrong preview", preview.compact("input"), Matchers.equalTo("input"));
        assertThat("Wrong preview", preview.full("input"), Matchers.equalTo("input"));

        assertThat("Wrong preview", preview.compact(null), Matchers.equalTo("null"));
        assertThat("Wrong preview", preview.full(null), Matchers.equalTo("null"));
    }

}
