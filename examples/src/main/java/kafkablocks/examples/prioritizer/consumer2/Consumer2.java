package kafkablocks.examples.prioritizer.consumer2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kafkablocks.examples.prioritizer.Constants.HIGH_PRIORITY_TOPIC;
import static kafkablocks.examples.prioritizer.Constants.LOW_PRIORITY_TOPIC;

@Service
@Slf4j
public class Consumer2 {
    @Autowired
    private BusinessService businessService;

    @KafkaListener(
            id = "consumer2-group",
            topics = {HIGH_PRIORITY_TOPIC, LOW_PRIORITY_TOPIC},
            containerFactory = "myFactory")
    public void getMessagesCalculatingOffset(@Payload String body,
                                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                             Consumer<?, ?> consumer,
                                             Acknowledgment ack) {

        if (HIGH_PRIORITY_TOPIC.equals(topic)) {
            businessService.processHigh(body);
            ack.acknowledge();
            return;
        }

        if (LOW_PRIORITY_TOPIC.equals(topic)) {
            log.info("checking unread...");
            var noUnread = noUnreadMessagesInTopic(HIGH_PRIORITY_TOPIC, consumer);
            log.info("checking unread...done");
            if (noUnread) {
                businessService.processLow(body);
                ack.acknowledge();
            } else {
                ack.nack(0);
            }
        }
    }

    private boolean noUnreadMessagesInTopic(String topic, Consumer<?, ?> consumer) {

        List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);

        Collection<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
            partitions.add(partition);
        }

        Map<TopicPartition, Long> endingOffsets = consumer.endOffsets(partitions);

        Map<TopicPartition, Long> currentOffsets = new HashMap<>();
        partitions.forEach(topicPartition -> currentOffsets.put(topicPartition, consumer.position(topicPartition)));

        return (currentOffsets.equals(endingOffsets));
    }
}
