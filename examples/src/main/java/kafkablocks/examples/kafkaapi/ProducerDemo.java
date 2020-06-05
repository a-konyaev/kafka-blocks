package kafkablocks.examples.kafkaapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafkablocks.events.Event;
import kafkablocks.examples.events.PositionEvent;
import kafkablocks.utils.ObjectMapperUtils;
import kafkablocks.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerDemo {
    private static final String TOPIC = "position";
    private final ObjectMapper mapper = ObjectMapperUtils.createWithDefaultDTFormatters();
    private final String objectId = UUID.randomUUID().toString();
    private int step = 0;

    public static void main(String[] args) {
        var producer = new ProducerDemo();
        producer.run();
    }

    public void run() {
        var props = getProperties();
        createTopic(TOPIC, 3, 1, props);
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 1000; i++) {
            var event = getPositionEvent();
            var record = getProducerRecord(event);
            var future = producer.send(record); // Async! adds the record to the buffer
            log.info("event published: {}", event);
            ThreadUtils.safeDelay(100);
        }
        producer.close();
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // wait confirmation from the leader only

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10*1024); // max size to flush (default = 16Kb)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 300_000); // max time to flush (default = 0)
        return props;
    }

    public void createTopic(String topic, int partitions, int replication, Properties props) {
        var topics = Collections.singletonList(new NewTopic(topic, partitions, (short) replication));
        try (AdminClient adminClient = AdminClient.create(props)) {
            var res = adminClient.createTopics(topics);
            res.all().get(); // wait for the topic will be created
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return; // topic already exists - ignore it
            }
            throw new RuntimeException(e);
        }
    }

    public PositionEvent getPositionEvent() {
        final double radius = 10;
        final double speed = 1.0;

        double radians = Math.PI / 180 * (++step) * speed;
        int x = (int) Math.round(radius * Math.sin(radians));
        int y = (int) Math.round(radius * Math.cos(radians));
        return new PositionEvent(objectId, x, y);
    }

    public ProducerRecord<String, String> getProducerRecord(Event event) {
        String data;
        try {
            data = mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization error", e);
        }
        return new ProducerRecord<>(TOPIC, event.getKey(), data);
    }
}
