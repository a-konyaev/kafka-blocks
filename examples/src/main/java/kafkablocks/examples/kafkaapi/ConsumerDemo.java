package kafkablocks.examples.kafkaapi;

import kafkablocks.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {
    private static final String TOPIC = "position";

    public static void main(String[] args) {
        var consumer = new ConsumerDemo();
        consumer.run();
    }

    public void run() {
        var props = getProperties();
        KafkaUtils.ensureTopicsExist(props, Collections.singleton(TOPIC), 10);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records)
                {
                    log.info("[{}] {} -> {}", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //default = latest
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //auto.commit.interval.ms
        //session.timeout.ms
        return props;
    }
}
