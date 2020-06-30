package kafkablocks.publisher;

import lombok.Setter;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import kafkablocks.events.Event;
import kafkablocks.EventTopicProperties;
import kafkablocks.serialization.SerdeProvider;

import javax.annotation.PostConstruct;
import java.util.UUID;

@Component
@EnableConfigurationProperties(EventTopicProperties.class)
public class KafkaPublisherImpl implements KafkaPublisher {
    @Setter
    private Logger logger = LoggerFactory.getLogger(KafkaPublisherImpl.class);
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;
    /**
     * Обертка для отправки сообщений в Кафку
     */
    private final KafkaTemplate<String, Event> kafkaTemplate;


    @Autowired
    public KafkaPublisherImpl(
            EventTopicProperties eventTopicProperties,
            KafkaProperties kafkaProperties,
            ProducerFactory<String, Event> producerFactory,
            KafkaAdmin admin) {

        this.eventTopicProperties = eventTopicProperties;
        this.kafkaProperties = kafkaProperties;
        this.kafkaTemplate = initKafkaTemplate(producerFactory);

        admin.setFatalIfBrokerNotAvailable(true);
    }

    @PostConstruct
    private void init() {
        eventTopicProperties.ensureTopicsExist(kafkaProperties);
    }

    private KafkaTemplate<String, Event> initKafkaTemplate(ProducerFactory<String, Event> producerFactory) {

        DefaultKafkaProducerFactory<String, Event> defaultKafkaProducerFactory =
                (DefaultKafkaProducerFactory<String, Event>) producerFactory;
        defaultKafkaProducerFactory.setKeySerializer(new StringSerializer());
        defaultKafkaProducerFactory.setValueSerializer(SerdeProvider.getSerializer());

        return new KafkaTemplate<>(producerFactory);
    }

    @Override
    public void publishEvent(Event event) {
        assertEvent(event);
        publishEvent(event, eventTopicProperties.resolveTopicByEvent(event), false);
    }

    @Override
    public void publishEvent(Event event, String topic) {
        assertEvent(event);
        publishEvent(event, topic, true);
    }

    private void publishEvent(Event event, String topic, boolean checkTopicExistence) {
        if (checkTopicExistence) {
            // todo проверять наличие топика.
            //  результаты проверки запоминать, чтобы не проверять постоянно
            //  и в эти результаты добавить топики, которые вычисляем автоматически
        }

        String id = event.getId();
        logger.debug("[{}] Sending event to topic '{}': {}", id, topic, event);

        // TODO: проанализировать результат и вернуть из текущего метода признак успешности отправки сообщения
        ListenableFuture<SendResult<String, Event>> future = kafkaTemplate.send(topic, event.getKey(), event);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Event>>() {
            @Override
            public void onSuccess(final SendResult<String, Event> message) {
                RecordMetadata recordMetadata = message.getRecordMetadata();
                logger.debug("[{}#{}] Event was successfully sent: partition={}, offset={}, key={}",
                        id,
                        event.getClass().getSimpleName(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        event.getKey());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                logger.error("[{}] Unable to send event", id, throwable);
            }
        });
    }

    private static void assertEvent(Event event) {
        if (event == null) {
            throw new IllegalArgumentException("event is null");
        }
    }
}
