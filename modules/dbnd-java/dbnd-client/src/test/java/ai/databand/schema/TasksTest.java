package ai.databand.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

class TasksTest {

    /**
     * Test java.time custom serialization.
     *
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void testDeserialize() throws IOException, URISyntaxException {
        String date = "2020-05-12T14:17:27.098000+00:00";
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSSSSSXXX");
        ZonedDateTime time = ZonedDateTime.parse(date, formatter);

        String data = new String(
            Files.readAllBytes(
                Paths.get(getClass().getClassLoader().getResource("tasks.json").toURI())
            )
        );
        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

        Tasks tasks = mapper.readValue(data, Tasks.class);
        MatcherAssert.assertThat("Tasks should not be empty", tasks, Matchers.notNullValue());
        MatcherAssert.assertThat("Tasks instances should not be empty", tasks.getTaskInstances(), Matchers.notNullValue());
    }

}
