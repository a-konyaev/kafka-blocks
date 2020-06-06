package kafkablocks.examples.springkafka.consumer;

import kafkablocks.examples.events.PositionEvent;
import kafkablocks.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class SpringConsumer {

    //@KafkaListener(id = "spring-kafka-consumer", topics = "spring-kafka-position")
    public void listen(String message) {
        log.info("message: {}", message);
    }

    //region fine-tuning

    @KafkaListener(id = "spring-kafka-consumer", topics = "spring-kafka-position", containerFactory = "myFactory")
    public void pollEvents(ConsumerRecords<String, PositionEvent> records) {
        log.info("Batch size = {}", records.count());
        for (ConsumerRecord<String, PositionEvent> record : records) {
            process(record.value());  // runtime error! record.value() - String!
            logRecord(record, "ok");
        }
        ThreadUtils.safeDelaySec(1);
    }

    private void logRecord(ConsumerRecord<String, PositionEvent> record, String prefix) {
        log.info("{} [{}-{}] {} -> {}", prefix, record.partition(), record.offset(), record.key(), record.value());
    }

    private void process(PositionEvent value) {
        ThreadUtils.safeDelay(300); // processing...
    }

    //region Acknowledgment

    //@KafkaListener(id = "spring-kafka-consumer", topics = "spring-kafka-position", containerFactory = "myFactory")
    public void pollEvents(List<ConsumerRecord<String, PositionEvent>> records, Acknowledgment ack) {
        int index = 0;
        try {
            for (; index < records.size(); index++) {
                var record = records.get(index);

                if (index >= 3) {
                    logRecord(record, "NOPE");
                    throw new RuntimeException("It is enough! No more than three on one!");
                }

                process(record.value());
                logRecord(record, "ok");
            }
        } catch (RuntimeException e) {
            // Error - commit only processed records, others will be consume again after 1 sec
            ack.nack(index, 1000);
        }
    }

    //endregion

    //endregion
}
