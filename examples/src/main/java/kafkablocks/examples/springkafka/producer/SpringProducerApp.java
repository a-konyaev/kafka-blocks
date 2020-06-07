package kafkablocks.examples.springkafka.producer;

import kafkablocks.events.Event;
import kafkablocks.examples.events.MultipleObjectsPositionEventGenerator;
import kafkablocks.examples.events.PositionEventGenerator;
import kafkablocks.utils.ObjectMapperUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@SpringBootApplication
public class SpringProducerApp {
    public static final String TOPIC = "spring-kafka-position";

    public static void main(String[] args) {
        SpringApplication.run(SpringProducerApp.class, args);
    }

    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate(ProducerFactory<String, Event> factory) {
        var defaultFactory = (DefaultKafkaProducerFactory<String, Event>) factory;
        defaultFactory.setKeySerializer(new StringSerializer());
        defaultFactory.setValueSerializer(new JsonSerializer<>(ObjectMapperUtils.createWithDefaultDTFormatters()));
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public NewTopic positionTopic() {
        return new NewTopic(TOPIC, 3, (short) 1);
    }

    @Bean
    public PositionEventGenerator positionEventFactory() {
        return new MultipleObjectsPositionEventGenerator(10);
    }
}
