package ai.databand.schema.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

public class ZonedDateTimeDeserializer extends StdDeserializer<ZonedDateTime> {

    private final String PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX";
    private final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(PATTERN);

    public ZonedDateTimeDeserializer() {
        this(null);
    }

    public ZonedDateTimeDeserializer(Class<ZonedDateTime> t) {
        super(t);
    }

    @Override
    public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String value = p.getValueAsString();
        try {
            return ZonedDateTime.parse(value, FORMATTER);
        } catch (Exception e) {
            return ZonedDateTime.parse(value, ISO_OFFSET_DATE_TIME);
        }
    }
}
