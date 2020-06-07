package kafkablocks.examples.springkafka.consumer;

import kafkablocks.examples.events.PositionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class SpringConsumerApp {

    public static void main(String[] args) {
        SpringApplication.run(SpringConsumerApp.class, args);
    }

    //region fine-tuning

//    @Bean
//    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, PositionEvent>>
//    myFactory(ConsumerFactory<String, PositionEvent> consumerFactory) {
//        var factory = new ConcurrentKafkaListenerContainerFactory<String, PositionEvent>();
//        factory.setConsumerFactory(consumerFactory);
//        factory.setBatchListener(true);
//        //factory.setConcurrency(4); // run in parallel
//
//        // for manual commits
//        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
//        return factory;
//    }

    //region deeper...
//    @Value("${spring.kafka.bootstrap-servers}")
//    private String bootstrapServers;
//
//    @Bean
//    public Map<String, Object> consumerConfigs() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//
//        // for manual commits
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        return props;
//    }
//
//    @Bean
//    @Primary
//    public ConsumerFactory<String, PositionEvent> consumerFactory() {
//        return new DefaultKafkaConsumerFactory<>(
//                consumerConfigs(),
//                new StringDeserializer(),
//                new JsonDeserializer<>(PositionEvent.class)); // exact type PositionEvent !!!
//    }
    //endregion

    //endregion
}
