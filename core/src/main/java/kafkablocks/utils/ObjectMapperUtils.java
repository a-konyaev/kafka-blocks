package kafkablocks.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Работа с ObjectMapper-ом
 */
public final class ObjectMapperUtils {
    private ObjectMapperUtils() {
    }

    private static final String DATETIME_PATTERN_FOR_SERIALIZE = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String DATETIME_PATTERN_FOR_DESERIALIZE = "yyyy-MM-dd'T'HH:mm:ss[.SSS][.SS][.S]";


    public static ObjectMapper create() {
        return create(null, null);
    }

    public static ObjectMapper createWithIsoZonedDTFormatters() {
        return create(DateTimeFormatter.ISO_ZONED_DATE_TIME, DateTimeFormatter.ISO_ZONED_DATE_TIME);
    }

    public static ObjectMapper createWithDefaultDTFormatters() {
        return create(
                DateTimeFormatter.ofPattern(DATETIME_PATTERN_FOR_SERIALIZE),
                DateTimeFormatter.ofPattern(DATETIME_PATTERN_FOR_DESERIALIZE));
    }

    public static ObjectMapper create(DateTimeFormatter forSerialize, DateTimeFormatter forDeserialize) {
        ObjectMapper mapper = new ObjectMapper();

        // регистронезависимый
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        // не падать, если встретим неизвестные поля
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // задаем форматтер для записи даты и времени
        if (forSerialize != null) {
            // время НЕ пишем как таймстамп
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapper.registerModule(new SimpleModule().addSerializer(
                    LocalDateTime.class, new LocalDateTimeSerializer(forSerialize)));
        }

        // задаем форматтер для парсинга даты и времени
        if (forDeserialize != null) {
            mapper.registerModule(new SimpleModule().addDeserializer(
                    LocalDateTime.class, new LocalDateTimeDeserializer(forDeserialize)));
        }

        return mapper;
    }
}
