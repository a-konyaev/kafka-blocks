package org.kafkablocks.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.kafkablocks.events.Event;
import org.kafkablocks.EventTopicProperties;
import org.kafkablocks.utils.ObjectMapperUtils;

import java.util.Map;


/**
 * Десериализатор для событий, который вычисляет тип события по имени топика с помощью EventTypeResolver-а
 * @param <T>
 */
public class EventDeserializer<T extends Event> implements Deserializer<T> {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * JSON-десериализатор
     * с включенным применением метаинформации для объектов типа Object и абстрактных классов
     */
    private final ObjectMapper objectMapper = ObjectMapperUtils
            .createWithDefaultDTFormatters();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    /**
     * todo: заменить json-сериализацию на Avro
     * вообще тогда надо делать так:
     * - при сериализации первые N байтов использовать как ИД Avro-схемы, которая и тип определяет и версию типа
     * - при десериализации читать эти первые N байтов и по ним в регистре находить avro-схему, которую использовать
     */
    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        try {
            Class<T> clazz = (Class<T>) EventTopicProperties.getInstance().resolveEventClassByTopic(topic);
            return objectMapper.readValue(data, clazz);
        } catch (Throwable e) {
            String eMsg = e.getMessage();
            String msg = String.format("Error of deserialization event from topic '%s': %s",
                    topic, eMsg == null ? e : eMsg);
            logger.error(msg, e);

            //todo: если мы здесь пульнем исключение, то KafkaConsumer зависнет -
            //  будет пытаться бесконечно обработать это кривое событие.
            // поэтому возвращаем null и в потребителе делаем проверку на null!
            //throw new SerializationException(msg, e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}
