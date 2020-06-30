package kafkablocks.consumer;

import kafkablocks.EventTopicProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@RequiredArgsConstructor
@EnableConfigurationProperties(EventTopicProperties.class)
public class KafkaConsumerFactory {
    private final EventTopicProperties eventTopicProperties;
    private final KafkaProperties kafkaProperties;

    @PostConstruct
    void init() {
        eventTopicProperties.ensureTopicsExist(kafkaProperties);
    }

    public KafkaConsumer createConsumer() {
        return new KafkaMultipleConsumer(eventTopicProperties, kafkaProperties);
    }
}
