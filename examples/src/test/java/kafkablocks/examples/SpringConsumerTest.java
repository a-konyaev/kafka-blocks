package kafkablocks.examples;

import kafkablocks.events.Event;
import kafkablocks.examples.events.PositionEvent;
import kafkablocks.examples.springkafka.consumer.SpringConsumer;
import kafkablocks.examples.springkafka.consumer.SpringConsumerApp;
import kafkablocks.examples.springkafka.producer.SpringProducerApp;
import kafkablocks.utils.ObjectMapperUtils;
import kafkablocks.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = SpringConsumerApp.class)
@ExtendWith(SpringExtension.class)
@Import(SpringConsumerTest.TestConfig.class)
@Slf4j
@ContextConfiguration(initializers = SpringConsumerTest.Initializer.class)
class SpringConsumerTest {

    static class Initializer extends KafkaInitializer {
        public Initializer() {
            super(SpringProducerApp.TOPIC, 1, (short) 1);
        }
    }

    @TestConfiguration
    public static class TestConfig {
        @Bean
        public KafkaTemplate<String, Event> kafkaTemplate(ProducerFactory<String, Event> factory) {
            var defaultFactory = (DefaultKafkaProducerFactory<String, Event>) factory;
            defaultFactory.setKeySerializer(new StringSerializer());
            defaultFactory.setValueSerializer(new JsonSerializer<>(ObjectMapperUtils.createWithDefaultDTFormatters()));
            return new KafkaTemplate<>(factory);
        }
    }

    @Autowired
    private KafkaTemplate<String, Event> kafkaTemplate;
    @SpyBean
    private SpringConsumer consumerSpy;

    @Test
    void allEventsProcessed() {
        ThreadUtils.safeDelaySec(1); // wait for partitions will be assigned to consumer

        var event = new PositionEvent("test-object", 0, 0);
        final int COUNT = 10;
        IntStream.range(0, COUNT).forEach(
                i -> kafkaTemplate.send(SpringProducerApp.TOPIC, event.getKey(), event)
        );

        ThreadUtils.safeDelay(COUNT * 300 + 500);
        verify(consumerSpy, times(COUNT)).process(any());
    }
}