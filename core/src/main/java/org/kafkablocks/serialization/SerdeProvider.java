package org.kafkablocks.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.kafkablocks.utils.ObjectMapperUtils;


/**
 * Factory for creating serializers / deserializers
 */
public class SerdeProvider {
    private static final ObjectMapper objectMapper = ObjectMapperUtils
            .createWithDefaultDTFormatters();

    // todo: заменить Json-сериализаторы на более производительные (см. KafkaAvroSerializer)
    public static <T> Serializer<T> getSerializer(){
        return new JsonSerializer<>(objectMapper);
    }

    public static <T> Deserializer<T> getDeserializer(Class<T> type){
        return new JsonDeserializer<>(type, objectMapper);
    }

    public static <T> Serde<T> getSerde(Class<T> type) {
        return Serdes.serdeFrom(getSerializer(), getDeserializer(type));
    }
}
